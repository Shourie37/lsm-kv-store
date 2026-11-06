from lsmkv.bench import run


def test_quick_benchmark_produces_markdown_tables():
    report = run(quick=True)
    assert report.count("### ") == 5
    assert "| leveled | none |" in report
    assert "space amp" in report
