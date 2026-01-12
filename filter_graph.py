
#!/usr/bin/env python3
import argparse
import json
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Any, Optional

import networkx as nx


def load_crawler_json(path: str, keep_external_targets: bool) -> Tuple[Dict[str, Any], Set[str], List[Tuple[str, str]]]:
    data = json.load(open(path, "r", encoding="utf-8"))

    nodes = set(data.keys())
    edges: List[Tuple[str, str]] = []

    for u, payload in data.items():
        for v in (payload.get("following") or []):
            if keep_external_targets:
                nodes.add(v)
                edges.append((u, v))
            else:
                if v in nodes:
                    edges.append((u, v))

    return data, nodes, edges


def build_digraph(nodes: Set[str], edges: List[Tuple[str, str]]) -> nx.DiGraph:
    G = nx.DiGraph()
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)
    return G


def get_reciprocal_edges(G: nx.DiGraph) -> Set[Tuple[str, str]]:
    # Return directed edges that are part of a mutual pair (u->v and v->u)
    recip = set()
    for u, v in G.edges():
        if G.has_edge(v, u):
            recip.add((u, v))
    return recip


def topk(items: List[Tuple[str, float]], k: int) -> List[str]:
    return [n for n, _ in sorted(items, key=lambda x: x[1], reverse=True)[:k]]


def induced_edge_cap(G: nx.DiGraph, keep_nodes: Set[str], max_edges: int) -> List[Tuple[str, str]]:
    """
    Build an edge list restricted to keep_nodes, capped by max_edges using a score:
      score = in_degree(target) + in_degree(source) + 3*(reciprocal)
    Keeps highest-score edges first.
    """
    sub = G.subgraph(keep_nodes).copy()
    indeg = dict(sub.in_degree())
    recip = get_reciprocal_edges(sub)

    scored = []
    for u, v in sub.edges():
        score = indeg.get(u, 0) + indeg.get(v, 0) + (3 if (u, v) in recip else 0)
        scored.append((score, u, v))

    scored.sort(reverse=True, key=lambda x: x[0])
    scored = scored[:max_edges]
    return [(u, v) for _, u, v in scored]


def adjacency_json_from_edges(
    nodes: Set[str],
    edges: List[Tuple[str, str]],
    root: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    following = defaultdict(list)
    for u, v in edges:
        following[u].append(v)

    # "linked_from": pick first parent discovered via BFS from root if provided,
    # else blank for all.
    linked_from = {n: "" for n in nodes}
    if root and root in nodes:
        q = [root]
        seen = {root}
        while q:
            u = q.pop(0)
            for v in following.get(u, []):
                if v not in seen:
                    seen.add(v)
                    linked_from[v] = u
                    q.append(v)

    out = {}
    for n in nodes:
        out[n] = {
            "following": sorted(following.get(n, [])),
            "linked_from": linked_from.get(n, ""),
        }
    return out


def write_processing_xml(nodes: Set[str], edges: List[Tuple[str, str]], out_xml: str) -> None:
    # Minimal Processing-compatible XML:
    # <graph><node id="..."/><edge source="..." target="..."/></graph>
    import xml.etree.ElementTree as ET
    from xml.dom import minidom

    root = ET.Element("graph")
    for n in sorted(nodes):
        ET.SubElement(root, "node", {"id": n})
    for u, v in edges:
        ET.SubElement(root, "edge", {"source": u, "target": v})

    rough = ET.tostring(root, encoding="utf-8")
    pretty = minidom.parseString(rough).toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")
    with open(out_xml, "w", encoding="utf-8") as f:
        f.write(pretty)


def main() -> int:
    ap = argparse.ArgumentParser(description="Filter a large Bluesky follow graph into a manageable backbone subgraph.")
    ap.add_argument("in_json", help="Input crawl output graph.json")
    ap.add_argument("--root", default=None, help="Root node id/handle (optional, for linked_from BFS)")
    ap.add_argument("--keep-external-targets", action="store_true",
                    help="Include edge targets even if they were not keys in the crawl JSON.")
    ap.add_argument("--top-in", type=int, default=200, help="Keep top-N nodes by in-degree within the dataset.")
    ap.add_argument("--top-bridge", type=int, default=120,
                    help="Keep top-N nodes by approximate betweenness (bridge score).")
    ap.add_argument("--betweenness-k", type=int, default=250,
                    help="Sample size for approximate betweenness (smaller=faster, 0 disables).")
    ap.add_argument("--keep-reciprocal-pairs", type=int, default=300,
                    help="Keep endpoints of top mutual-follow pairs ranked by combined in-degree.")
    ap.add_argument("--ego-hops", type=int, default=0,
                    help="Additionally keep nodes within N hops of --root (0 disables).")
    ap.add_argument("--max-nodes", type=int, default=800,
                    help="Hard cap on kept nodes after union (trim by priority scores).")
    ap.add_argument("--max-edges", type=int, default=20000,
                    help="Hard cap on edges in output (keeps highest-scoring edges).")
    ap.add_argument("--out-json", default="filtered_graph.json", help="Output filtered adjacency JSON.")
    ap.add_argument("--out-xml", default=None, help="Optional Processing XML output path.")
    args = ap.parse_args()

    raw, nodes, edges = load_crawler_json(args.in_json, keep_external_targets=args.keep_external_targets)
    G = build_digraph(nodes, edges)

    indeg = dict(G.in_degree())
    outdeg = dict(G.out_degree())

    # 1) top in-degree (hubs)
    hubs = set(topk([(n, float(indeg.get(n, 0))) for n in G.nodes()], args.top_in))

    # 2) approximate betweenness (bridges)
    bridges = set()
    btw = {}
    if args.betweenness_k > 0 and args.top_bridge > 0 and G.number_of_nodes() > 0:
        k = min(args.betweenness_k, G.number_of_nodes())
        btw = nx.betweenness_centrality(G, k=k, normalized=True, seed=1)
        bridges = set(topk([(n, float(btw.get(n, 0.0))) for n in G.nodes()], args.top_bridge))

    # 3) reciprocal pair endpoints
    recip_edges = get_reciprocal_edges(G)
    # rank mutual pairs by combined in-degree; store undirected pairs once
    seen_pairs = set()
    pair_scores = []
    for u, v in recip_edges:
        a, b = (u, v) if u <= v else (v, u)
        if (a, b) in seen_pairs:
            continue
        seen_pairs.add((a, b))
        score = indeg.get(a, 0) + indeg.get(b, 0)
        pair_scores.append((score, a, b))
    pair_scores.sort(reverse=True, key=lambda x: x[0])
    top_pairs = pair_scores[: args.keep_reciprocal_pairs]
    reciprocal_nodes = set()
    for _, a, b in top_pairs:
        reciprocal_nodes.add(a)
        reciprocal_nodes.add(b)

    # 4) ego network around root (optional)
    ego = set()
    if args.root and args.ego_hops > 0 and args.root in G:
        # use undirected neighborhood for “graph shape”
        UG = G.to_undirected()
        ego.add(args.root)
        frontier = {args.root}
        for _ in range(args.ego_hops):
            nxt = set()
            for u in frontier:
                nxt |= set(UG.neighbors(u))
            ego |= nxt
            frontier = nxt

    keep = set()
    keep |= hubs
    keep |= bridges
    keep |= reciprocal_nodes
    keep |= ego
    if args.root and args.root in G:
        keep.add(args.root)

    # Priority trim if too many nodes
    if len(keep) > args.max_nodes:
        def priority(n: str) -> float:
            # hubs + bridges + reciprocal endpoints + ego are weighted
            p = 0.0
            p += indeg.get(n, 0) * 1.0
            p += outdeg.get(n, 0) * 0.1
            p += btw.get(n, 0.0) * 5000.0
            if n in reciprocal_nodes:
                p += 50.0
            if n in ego:
                p += 25.0
            if args.root and n == args.root:
                p += 1e9
            return p

        keep = set(sorted(list(keep), key=priority, reverse=True)[: args.max_nodes])

    # Edge cap inside induced subgraph
    kept_edges = induced_edge_cap(G, keep, args.max_edges)

    # Emit filtered adjacency JSON (same schema as your crawler)
    out = adjacency_json_from_edges(keep, kept_edges, root=args.root)
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=True)

    if args.out_xml:
        write_processing_xml(keep, kept_edges, args.out_xml)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
