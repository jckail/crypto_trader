#!/usr/bin/env python3
"""Parse active syntax and documentation without importing historical ingestion modules."""
import ast
from pathlib import Path
import re
import sys
from urllib.parse import unquote, urlsplit

EXPERIMENTS = ['alpha/avcoinlist.py', 'alpha/avcurexchangerate.py', 'alpha/avcurrencylist.py', 'alpha/avmarketlist.py', 'alpha/awscatalogcreate.py', 'alpha/coinlist.py', 'alpha/coinmarketcaptest.py', 'alpha/day_hist.py', 'alpha/fetchprice.py', 'alpha/gluemaintenance.py', 'alpha/hour_hist.py', 'alpha/logtos3.py', 'alpha/miningdata.py', 'alpha/minute_hist.py', 'alpha/mt_alpha_runner.py', 'alpha/mtsocial.py', 'alpha/quadlcmedata.py', 'alpha/quandlgetlmedata.py', 'alpha/quandllbma.py', 'alpha/quandlstockdata.py', 'alpha/s3maintenance.py', 'alpha/savetos3.py', 'alpha/setup.py', 'alpha/tradepair.py', 'alpha/validatedatabase.py', 'omega/athena_test.py', 'omega/backwardelimination_rsquared_test.py', 'omega/simpleregressiontest.py']
INACTIVE_FAILURES = {'alpha/inactives/forloopfetchprice.py': 64, 'alpha/inactives/haspricing.py': 46, 'alpha/inactives/newfetchprice.py': 71, 'alpha/inactives/newtradepair.py': 79, 'alpha/inactives/price_all_permutation.py': 68}

def check(root):
    errors = []
    discovered = {str(p.relative_to(root)) for directory in ["alpha", "omega"]
                  for p in (root / directory).glob("*.py")}
    if discovered != set(EXPERIMENTS):
        errors.append("Experiment inventory changed; review guide and checker coverage")
    for name in EXPERIMENTS:
        try:
            ast.parse((root / name).read_text(encoding="utf-8"), filename=name)
        except (OSError, SyntaxError) as error:
            # Error metadata only; source lines/configuration literals stay local.
            errors.append(f"{name}: syntax/read failure ({type(error).__name__}, line {getattr(error, 'lineno', None)})")
    failures = {}
    for path in (root / "alpha/inactives").glob("*.py"):
        try:
            ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as error:
            failures[str(path.relative_to(root))] = error.lineno
    if failures != INACTIVE_FAILURES:
        errors.append("Historical inactive syntax gaps changed; review documentation")
    for name in ["README.md", "docs/architecture.mdx", "docs/developer-guide.mdx"]:
        path = root / name
        opened = False
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if line.startswith("```"):
                if not opened and not line[3:].strip():
                    errors.append(f"{name}:{number}: unlabelled fence")
                opened = not opened
            if opened:
                continue
            for link in re.findall(r"\[[^\]]+\]\(([^)]+)\)", line):
                url = urlsplit(link)
                if url.scheme or url.netloc or not url.path:
                    continue
                target = (path.parent / unquote(url.path)).resolve()
                if not target.is_relative_to(root) or not target.exists():
                    errors.append(f"{name}:{number}: missing local link {link}")
        if opened:
            errors.append(f"{name}: unclosed fence")
    return errors


if __name__ == "__main__":
    root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parents[1]
    errors = check(root)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        sys.exit(1)
    print("25 alpha and three omega scripts parse; documented inactive gaps and docs checks pass.")
    print("No module import, data read or external operation; not runtime or MDX qualification.")
