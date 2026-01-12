#!/usr/bin/env python3
import argparse
import json
from collections import Counter, defaultdict
from itertools import combinations
import math

import networkx as nx


def load_graph(path: str):
    data = json.load(open(path, "r", encoding="utf-8"))
    # Keep only edges where target exists in dataset to avoid phantom nodes (optional)
    nodes = set(data.keys())
    edges = []
    for u, payload in data.items():
        for v in payload.get("following", []):
            if v in nodes:
                edges.append((u, v))
    return data, nodes, edges


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 0.0
    inter = len(a & b)
    uni = len(a | b)
    return inter / uni if uni else 0.0


def main():
    ap = argparse.ArgumentParser(description="Analyze Bluesky follow graph for structure; output JSON.")
    ap.add_argument("in_json", help="crawler output graph.json")
    ap.add_argument("--min-shared", type=int, default=10, help="Min shared follow targets for co-follow edges")
    ap.add_argument("--top", type=int, default=50, help="How many results per report section")
    ap.add_argument("--approx-betweenness-k", type=int, default=200,
                    help="Approx betweenness sample size (0 disables)")
    ap.add_argument("--out", default="analysis.json", help="Output JSON report")
    args = ap.parse_args()

    _, nodes, edges = load_graph(args.in_json)

    G = nx.DiGraph()
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)

    # Degree
    in_deg = dict(G.in_degree())
    out_deg = dict(G.out_degree())

    top_in = sorted(in_deg.items(), key=lambda x: x[1], reverse=True)[: args.top]
    top_out = sorted(out_deg.items(), key=lambda x: x[1], reverse=True)[: args.top]

    # Reciprocity pairs
    reciprocals = []
    seen = set()
    for u, v in G.edges():
        if (v, u) in G.edges() and (v, u) not in seen:
            reciprocals.append((u, v))
            seen.add((u, v))
            seen.add((v, u))

    # Rank reciprocals by "importance" (sum of in-degrees)
    reciprocal_ranked = sorted(
        [{"a": a, "b": b, "score": in_deg.get(a, 0) + in_deg.get(b, 0)} for a, b in reciprocals],
        key=lambda x: x["score"],
        reverse=True,
    )[: args.top]

    # Build following sets
    following = {u: set(G.successors(u)) for u in nodes}

    # Co-follow similarity using inverted index
    # target -> list of followers of target
    inv = defaultdict(list)
    for u in nodes:
        for tgt in following[u]:
            inv[tgt].append(u)

    # Count shared follows per pair without O(n^2) over nodes
    shared_counts = Counter()
    for tgt, followers in inv.items():
        if len(followers) < 2:
            continue
        # If a target has huge follower list, combinations explode.
        # Cap to control worst-case blowups.
        followers = followers[:5000]
        for a, b in combinations(sorted(followers), 2):
            shared_counts[(a, b)] += 1

    # Filter and compute Jaccard for top pairs
    cofollow = []
    for (a, b), c in shared_counts.most_common():
        if c < args.min_shared:
            break
        ja = jaccard(following[a], following[b])
        cofollow.append({"a": a, "b": b, "shared": c, "jaccard": ja})
        if len(cofollow) >= args.top:
            break

    # Approx betweenness centrality (bridges)
    betweenness = {}
    if args.approx_betweenness_k > 0:
        k = min(args.approx_betweenness_k, len(nodes))
        # networkx betweenness works on DiGraph; approximation uses k samples
        betweenness = nx.betweenness_centrality(G, k=k, normalized=True, seed=1)
    top_bridge = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[: args.top] if betweenness else []

    # Strongly connected components (SCCs) - dense mutual-follow subgraphs
    sccs = list(nx.strongly_connected_components(G))
    sccs_sorted = sorted(sccs, key=len, reverse=True)
    top_sccs = [
        {"size": len(comp), "nodes": sorted(list(comp))[:50]}
        for comp in sccs_sorted[:10]
        if len(comp) > 1
    ]

    report = {
        "summary": {
            "nodes": len(nodes),
            "edges": len(edges),
            "reciprocal_pairs": len(reciprocals),
        },
        "top_in_degree": [{"node": n, "in_degree": d} for n, d in top_in],
        "top_out_degree": [{"node": n, "out_degree": d} for n, d in top_out],
        "top_reciprocals": reciprocal_ranked,
        "top_cofollow_pairs": cofollow,
        "top_bridges_betweenness": [{"node": n, "betweenness": v} for n, v in top_bridge],
        "top_strongly_connected_components": top_sccs,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

