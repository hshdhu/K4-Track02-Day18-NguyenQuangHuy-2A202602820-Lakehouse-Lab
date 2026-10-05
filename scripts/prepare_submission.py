"""Execute real notebook cells and preserve their outputs for review."""
from pathlib import Path
import os
import sys
import jupytext
import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "submission" / "notebooks"

EXPLANATIONS = [
    "Enforcement kiểm tra dữ liệu ghi theo schema hiện tại, nên age='thirty' bị chặn. Evolution cho phép thêm tier khi người ghi bật schema_mode='merge'; opt-in tránh vô tình thay đổi hợp đồng dữ liệu. Các dòng cũ có tier NULL, dòng mới có tier premium. Commit JSON ghi metadata/schema, add chứa đường dẫn và thống kê file, commitInfo ghi thao tác: đây là bằng chứng trạng thái được commit, không phải chỉ có file Parquet trên đĩa.",
    "Compaction gộp file nhỏ để giảm chi phí mở file; Z-order tổ chức dữ liệu theo user_id để min/max loại được file không chứa user cần tìm. Nếu chỉ còn một file lớn thì gần như không còn file để skip. Thời gian phụ thuộc cache, SSD, CPU và tải máy; dùng median nhiều lượt và đối chiếu pruning ratio với ngưỡng thay vì suy luận từ một lần đo. Các số trước/sau và min/max trong output phía trên là kết quả của lần chạy này.",
    "MERGE 100K dòng nguồn gồm 50K update và 50K insert. Time travel đọc một version cũ nhưng không đổi trạng thái hiện tại. RESTORE về v2 tạo v4 với trạng thái của v2, loại các dòng score âm khỏi bảng hiện tại. Lịch sử vẫn có v0–v4 và RESTORE; transaction mới giữ audit trail và cho reader theo dõi thay đổi nhất quán.",
    "Silver dedup theo request_id để retry không bị tính nhiều lần trong latency, token và chi phí. Dashboard đọc Gold vì Gold đã tổng hợp theo ngày/model, giảm công việc truy vấn lặp lại. Error rate là tỷ lệ status khác ok trên các request đã dedup; chi phí = input tokens × đơn giá input/1e6 + output tokens × đơn giá output/1e6. Phù hợp dữ liệu giả và bảng giá minh họa của lab; không phải hóa đơn thực tế. Query hiện tại không tự bỏ mọi JSON malformed: json_extract có thể lỗi với JSON không hợp lệ, nên production cần json_valid hoặc TRY cùng kiểm tra kiểu và giá trị NULL.",
    "Hidden partitioning chuyển predicate trên ts thành điều kiện day(ts) để plan_files bỏ partition không liên quan; người dùng không phải tự lọc cột ts_day. Rename giữ field_id=4 nên file cũ vẫn ánh xạ đúng cột dù tên đổi. Mỗi file được gắn spec ID; planner hiểu cả spec cũ và mới, nên partition evolution không bắt buộc rewrite toàn bộ dữ liệu. Tỷ lệ metadata:data trong output phản ánh overhead của nhiều file nhỏ; đây là catalog SQLite và client-side planning cục bộ.",
    "Compaction giảm số file active nhưng tạo file mới trước khi vacuum thu hồi file cũ. Clustering làm min/max hữu ích cho point query. Delta vacuum trong phiên bản lab xét các file đã commit rồi bị remove; orphan chưa từng commit cần sweep riêng, có age guard để tránh xóa file writer đang ghi. PyIceberg expiry ở đường chạy này giảm snapshot trong metadata, chưa tự xóa toàn bộ file vật lý; sweep manifest list không còn tham chiếu mới thu hồi bytes. Retention quá ngắn làm reader đang dùng version/snapshot cũ mất file. Retention 0 ở đây chỉ cho scratch; checkpoint tăng tốc dựng trạng thái, không thay thế backup.",
    "Random read blob inline phải đọc phần blob của row group nên có read amplification; pointer lấy riêng object, nhưng analytics có projection pushdown vẫn có thể tránh đọc cột blob inline. Int8 giảm bytes, đổi lại sai số lượng tử có thể đổi thứ tự hàng xóm. Recall@10 đo giao nhau theo doc ID với top-10 float32; topic fidelity đo tỷ lệ hàng xóm cùng topic, vì vậy hai metric không tương đương. DuckDB cast list embedding sang FLOAT[dim] để tính cosine bằng SQL. Sau delete, bảng có 0 hits nhưng index cũ vẫn trả dữ liệu: index cần CDF delete (và insert/update), xử lý idempotent, lưu offset và có cơ chế rebuild/đối soát.",
    "Pin table version giữ snapshot dữ liệu training trước các append, giúp replay có đầu vào xác định. Lab mới so số bước, chưa chứng minh toàn bộ nội dung giống nhau; cần hash/nội dung và pin cả code, config, seed. Delete ở version hiện tại không xóa file còn được version cũ tham chiếu. Production cần retention/vacuum phối hợp reader, cache và index. Mô phỏng MCP này chạy offline: confirmed do caller truyền không phải authorization, polling không phải job nền bền vững; cache cần invalidation và kiểm soát quyền. Bốn bucket provenance và UNCLASSIFIED chỉ là mapping minh họa, không chứng minh quyền sử dụng dữ liệu; filter trainable loại UNCLASSIFIED theo quy tắc lab.",
]

EXTRA = {
    1: '''from pathlib import Path
import json
logs = sorted(Path(table_path).glob("_delta_log/*.json"))
print("Commit JSON files:", [p.name for p in logs])
print(logs[0].read_text(encoding="utf-8"))
print("Schema after evolution:", DeltaTable(table_path).schema())
''',
    4: '''g = gold_df
assert n_models == 3 and g.height == n_dates * n_models
assert g.filter(pl.col("p50_latency_ms") > pl.col("p95_latency_ms")).height == 0
assert g.filter(pl.col("cost_usd") <= 0).height == 0
assert g.filter(~pl.col("error_rate").is_between(0, 1)).height == 0
assert all(Path(p).exists() for p in [BRONZE, SILVER, GOLD])
with pl.Config(tbl_rows=30, tbl_cols=8, tbl_width_chars=150):
    print(g.sort(["date", "model"]))
print("Gold checks PASS: complete date/model grid, p50<=p95, positive cost, error rate [0,1]")
print("Storage:", BRONZE, SILVER, GOLD, sep="\\n")
''',
    6: '''print("Current Delta rows still readable:", DeltaTable(TABLE).to_pyarrow_table().num_rows)
assert DeltaTable(TABLE).to_pyarrow_table().num_rows == N_BATCHES * ROWS_PER_BATCH
''',
}

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    (ROOT / "submission" / "screenshots").mkdir(exist_ok=True)
    for i, source in enumerate(sorted((ROOT / "notebooks").glob("[0-9]*.py")), 1):
        nb = jupytext.read(source)
        # Submission notebooks are standalone .ipynb files, not paired sources.
        nb.metadata.pop("jupytext", None)
        nb.cells.insert(1, nbformat.v4.new_code_cell('''import sys
from pathlib import Path
root = Path.cwd()
while not (root / "scripts" / "lakehouse.py").exists():
    if root == root.parent:
        raise RuntimeError("Cannot find repository root")
    root = root.parent
sys.path.insert(0, str(root / "notebooks"))
print("Kernel Python:", sys.executable)
'''))
        if i in EXTRA:
            nb.cells.append(nbformat.v4.new_markdown_cell("## Bằng chứng bổ sung và kiểm tra rubric"))
            nb.cells.append(nbformat.v4.new_code_cell(EXTRA[i]))
        nb.cells.append(nbformat.v4.new_markdown_cell("## Diễn giải kết quả — bản nháp cần người nộp rà soát\n\n" + EXPLANATIONS[i-1]))
        nb.metadata.kernelspec = {"name": "lab18", "display_name": "Python (.venv Lab18)", "language": "python"}
        client = NotebookClient(nb, kernel_name="lab18", timeout=600, resources={"metadata": {"path": str(ROOT / "notebooks")}})
        os.environ["JUPYTER_PATH"] = str(ROOT / ".venv" / "share" / "jupyter")
        with client.setup_kernel():
            client.reset_execution_trackers()
            client.execute_cell(nbformat.v4.new_code_cell("import sys\nprint(sys.executable)"), -1)
            for index, cell in enumerate(nb.cells):
                client.execute_cell(cell, index)
        nbformat.write(nb, OUT / (source.stem + ".ipynb"))
        print("SAVED", source.stem, flush=True)

if __name__ == "__main__":
    main()
