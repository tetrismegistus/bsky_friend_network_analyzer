#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

from atproto import Client


def env_optional(name: str) -> Optional[str]:
    v = os.environ.get(name)
    if not v:
        return None
    v = v.strip()
    return v or None


def as_dict(obj: Any) -> Dict[str, Any]:
    # atproto-python often returns pydantic models
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    # last resort
    return dict(obj)


def normalize_actor_view(actor_view: Dict[str, Any]) -> Tuple[str, str]:
    """
    Return (name, did).
    Prefer handle as 'name' if present; otherwise use DID.
    """
    did = actor_view.get("did", "") or ""
    handle = actor_view.get("handle", "") or ""
    name = handle if handle else (did if did else "unknown")
    return name, did


def get_all_follows(
    client: Client,
    actor: str,
    *,
    per_page: int = 100,
    max_items: Optional[int] = None,
    min_pause_s: float = 0.0,
    retries: int = 5,
) -> List[Dict[str, Any]]:
    """
    Fetch all accounts `actor` follows (app.bsky.graph.getFollows), handling cursor pagination.
    Returns list of profile views.
    """
    out: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        # Simple retry loop (handles transient errors / rate limiting)
        last_exc: Optional[Exception] = None
        for attempt in range(retries):
            try:
                params = {"actor": actor, "limit": per_page}
                if cursor:
                    params["cursor"] = cursor
                resp = client.app.bsky.graph.get_follows(params)
                page = as_dict(resp)
                break
            except Exception as e:
                last_exc = e
                # Exponential-ish backoff
                sleep_s = min(2 ** attempt, 30)
                time.sleep(sleep_s)
        else:
            raise RuntimeError(f"Failed fetching follows for {actor}: {last_exc}")

        follows = page.get("follows", []) or []
        # Ensure dict form
        follows = [as_dict(x) for x in follows]
        out.extend(follows)

        if max_items is not None and len(out) >= max_items:
            return out[:max_items]

        cursor = page.get("cursor")
        if min_pause_s > 0:
            time.sleep(min_pause_s)
        if not cursor:
            return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Crawl Bluesky follows graph to N levels, output JSON.")
    ap.add_argument("root_actor", help="Root account handle or DID (e.g. you.bsky.social or did:plc:...)")
    ap.add_argument("--depth", type=int, default=1, help="Levels deep (1 = root's following only).")
    ap.add_argument("--per-page", type=int, default=100, help="Pagination page size (<=100 is typical).")
    ap.add_argument("--max-accounts", type=int, default=5000,
                    help="Hard cap on total unique accounts collected (safety valve).")
    ap.add_argument("--max-follows-per-account", type=int, default=None,
                    help="Optional cap on follows fetched per account (limits blast radius).")
    ap.add_argument("--pause", type=float, default=0.0,
                    help="Optional pause between page fetches (seconds). Helpful for rate limits.")
    ap.add_argument("--auth", action="store_true",
                    help="Use auth from env (BSKY_HANDLE / BSKY_APP_PASSWORD). Not required for public data.")
    args = ap.parse_args()

    if args.depth < 0:
        print("depth must be >= 0", file=sys.stderr)
        return 2

    client = Client()
    if args.auth:
        h = env_optional("BSKY_HANDLE")
        p = env_optional("BSKY_APP_PASSWORD")
        if not (h and p):
            print("Missing env vars for --auth: BSKY_HANDLE and/or BSKY_APP_PASSWORD", file=sys.stderr)
            return 2
        client.login(h, p)

    # Graph store: name -> { following: [names], linked_from: parent_name, did: ... }
    graph: Dict[str, Dict[str, Any]] = {}

    visited: set[str] = set()     # by DID when possible; fallback to name string
    name_by_id: Dict[str, str] = {}  # id -> canonical name
    id_by_name: Dict[str, str] = {}  # canonical name -> id

    def record_node(name: str, did: str, linked_from: str) -> None:
        node = graph.get(name)
        if node is None:
            graph[name] = {
                "following": [],
                "linked_from": linked_from,
                "did": did,
            }
        else:
            # Keep the first linked_from (BFS discovery parent)
            if node.get("linked_from") in (None, "") and linked_from:
                node["linked_from"] = linked_from
            if did and not node.get("did"):
                node["did"] = did

    # Seed root: try to resolve via getProfile to canonical handle/DID
    root_name = args.root_actor
    root_did = ""
    try:
        prof = as_dict(client.app.bsky.actor.get_profile({"actor": args.root_actor}))
        root_name, root_did = normalize_actor_view(prof)
    except Exception:
        # If it fails, proceed with whatever they passed
        root_name = args.root_actor
        root_did = ""

    root_id = root_did or root_name
    visited.add(root_id)
    name_by_id[root_id] = root_name
    id_by_name[root_name] = root_id

    record_node(root_name, root_did, linked_from="")

    # BFS queue items: (actor_name, actor_id, current_depth)
    q = deque([(root_name, root_id, 0)])

    while q:
        actor_name, actor_id, d = q.popleft()
        if d >= args.depth:
            continue

        # Safety cap
        if args.max_accounts is not None and len(visited) >= args.max_accounts:
            break

        # Fetch who this actor follows
        follows_views = get_all_follows(
            client,
            actor_name if actor_name.startswith("did:") is False else actor_id,
            per_page=args.per_page,
            max_items=args.max_follows_per_account,
            min_pause_s=args.pause,
        )

        following_names: List[str] = []
        for fv in follows_views:
            nm, did = normalize_actor_view(fv)
            follower_id = did or nm
            following_names.append(nm)

            # Record the node and edge
            record_node(nm, did, linked_from=actor_name)

            # Dedupe + enqueue
            if follower_id not in visited:
                if args.max_accounts is not None and len(visited) >= args.max_accounts:
                    break
                visited.add(follower_id)
                name_by_id[follower_id] = nm
                id_by_name[nm] = follower_id
                q.append((nm, follower_id, d + 1))

        graph[actor_name]["following"] = following_names

    # JSON only to stdout
    json.dump(graph, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

