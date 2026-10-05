# Reflection

Anti-pattern tôi chọn là coi external vector index như nguồn dữ liệu chính và bỏ qua đồng bộ vòng đời dữ liệu. Với hệ thống tìm kiếm tài liệu cho trợ lý học tập, xóa tài liệu khỏi lakehouse chưa đủ: index cũ vẫn có thể trả về nội dung đã bị xóa. NB7 tái hiện vấn đề bằng hai truy vấn sau delete.

Tôi đề xuất giữ lakehouse làm nguồn chuẩn và coi index là dữ liệu dẫn xuất có thể xây lại. Pipeline cần xử lý insert, update và delete từ CDF theo doc ID, lưu offset, áp dụng idempotent và đối soát định kỳ. Khi đồng bộ lỗi, hệ thống cần cảnh báo và kiểm tra quyền/trạng thái tài liệu trước khi trả kết quả. Retention phải đủ dài để consumer bắt kịp; nếu mất lịch sử CDF thì rebuild index từ snapshot hợp lệ.

Tôi dùng OpenAI Codex để hỗ trợ kiểm tra môi trường, thực thi notebook, bổ sung kiểm tra, soạn bản nháp giải thích/reflection và hướng dẫn chụp ảnh.
