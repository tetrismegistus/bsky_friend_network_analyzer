
#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> None:
    print("+", " ".join(cmd), file=sys.stderr)
    subprocess.run(cmd, check=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Bluesky graph pipeline: crawl -> xml -> analysis -> filtered graph/xml.")
    ap.add_argument("actor", help="Root handle/DID (e.g. taelar.bsky.social)")
    ap.add_argument("max_accounts", type=int, help="Max accounts to crawl")
    ap.add_argument("--depth", type=int, default=3, help="Crawl depth (default 2)")
    ap.add_argument("--auth", action="store_true", default=True, help="Use auth (default true)")
    ap.add_argument("--no-auth", dest="auth", action="store_false", help="Disable auth")
    ap.add_argument("--out-dir", default="out", help="Output directory")
    ap.add_argument("--min-shared", type=int, default=8, help="Analyze: min shared follows")
    ap.add_argument("--top", type=int, default=50, help="Analyze: top N results")
    ap.add_argument("--betweenness-k", type=int, default=200, help="Analyze: approx betweenness sample size")
    # Filter params
    ap.add_argument("--ego-hops", type=int, default=2)
    ap.add_argument("--top-in", type=int, default=200)
    ap.add_argument("--top-bridge", type=int, default=120)
    ap.add_argument("--keep-reciprocal-pairs", type=int, default=300)
    ap.add_argument("--max-nodes", type=int, default=800)
    ap.add_argument("--max-edges", type=int, default=15000)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    graph_json = out_dir / "graph.json"
    markov_xml = out_dir / "markov_chain.xml"
    analysis_json = out_dir / "analysis.json"
    filtered_json = out_dir / "filtered_graph.json"
    filtered_xml = out_dir / "filtered.xml"

    # 1) Crawl
    crawl_cmd = [
        sys.executable, "bsky_friend_network_analyzer.py",
        args.actor,
        "--depth", str(args.depth),
        "--max-accounts", str(args.max_accounts),
    ]
    if args.auth:
        crawl_cmd.append("--auth")

    # IMPORTANT: write graph.json to file directly (avoid shell redirection)
    print("+", " ".join(crawl_cmd), ">", str(graph_json), file=sys.stderr)
    with open(graph_json, "w", encoding="utf-8") as f:
        subprocess.run(crawl_cmd, stdout=f, check=True)

    # 2) JSON -> Processing XML (raw)
    run([sys.executable, "json_to_processing_xml.py", str(graph_json), str(markov_xml),
         "--dedupe-edges", "--include-external-nodes"])

    # 3) Analyze
    run([sys.executable, "analyze_graph.py", str(graph_json),
         "--min-shared", str(args.min_shared),
         "--top", str(args.top),
         "--approx-betweenness-k", str(args.betweenness_k),
         "--out", str(analysis_json)])

    # 4) Filter backbone + Processing XML (filtered)
    run([
        sys.executable, "filter_graph.py", str(graph_json),
        "--root", args.actor,
        "--ego-hops", str(args.ego_hops),
        "--top-in", str(args.top_in),
        "--top-bridge", str(args.top_bridge),
        "--keep-reciprocal-pairs", str(args.keep_reciprocal_pairs),
        "--max-nodes", str(args.max_nodes),
        "--max-edges", str(args.max_edges),
        "--out-json", str(filtered_json),
        "--out-xml", str(filtered_xml),
    ])

    print(f"\nOutputs in: {out_dir}", file=sys.stderr)
    print(f"- {graph_json}", file=sys.stderr)
    print(f"- {markov_xml}", file=sys.stderr)
    print(f"- {analysis_json}", file=sys.stderr)
    print(f"- {filtered_json}", file=sys.stderr)
    print(f"- {filtered_xml}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
