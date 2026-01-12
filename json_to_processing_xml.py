#!/usr/bin/env python3
import argparse
import json
import xml.etree.ElementTree as ET
from xml.dom import minidom

def prettify(elem: ET.Element) -> str:
    rough = ET.tostring(elem, encoding="utf-8")
    reparsed = minidom.parseString(rough)
    return reparsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")

def main() -> int:
    ap = argparse.ArgumentParser(description="Convert crawler JSON to Processing XML (graph/node + graph/edge).")
    ap.add_argument("in_json", help="Input JSON from crawler (graph.json)")
    ap.add_argument("out_xml", help="Output XML for Processing (graph.xml)")
    ap.add_argument("--include-external-nodes", action="store_true",
                    help="If an edge points to a node not present as a key in JSON, include it as a node anyway.")
    ap.add_argument("--dedupe-edges", action="store_true", help="Remove duplicate edges.")
    args = ap.parse_args()

    data = json.load(open(args.in_json, "r", encoding="utf-8"))

    root = ET.Element("graph")

    # Node set
    node_ids = set(data.keys())
    if args.include_external_nodes:
        for src, payload in data.items():
            for tgt in (payload.get("following") or []):
                node_ids.add(tgt)

    # Emit nodes
    for node_id in sorted(node_ids):
        ET.SubElement(root, "node", {"id": node_id})

    # Emit edges
    seen = set()
    for src, payload in data.items():
        for tgt in (payload.get("following") or []):
            if (not args.include_external_nodes) and (tgt not in node_ids):
                continue
            edge = (src, tgt)
            if args.dedupe_edges:
                if edge in seen:
                    continue
                seen.add(edge)
            ET.SubElement(root, "edge", {"source": src, "target": tgt})

    xml_str = prettify(root)
    with open(args.out_xml, "w", encoding="utf-8") as f:
        f.write(xml_str)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())

