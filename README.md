# Sales & Marketing Performance Dashboard

Dashboard theo dõi hiệu suất Sales & Marketing — từ doanh số, phễu tư vấn đến quảng cáo. Mục tiêu là giúp team xác định đúng vấn đề và hành động kịp thời.

## [Xem Interactive Dashboard tại đây](https://report.onhandbi.com/public/report?token=eyJhbGciOiJIUzI1NiJ9.eyJwdWJsaWNfbGlua19pZCI6NjUyLCJoYXNfcGFzc2NvZGUiOmZhbHNlLCJ0aW1lIjoxNzc5NDI0ODEyfQ.HAXS6eA6CcDFiJ4y_V1-pdeQlnpXUF862bWl-kZ7APk)

---

## Kiến Trúc Pipeline

Data được thu thập từ 5 nguồn, xử lý qua Python, lưu vào SQL Server và trực quan hóa trên Power BI. Toàn bộ pipeline được orchestrate và schedule tự động bằng Dagster.

```mermaid
flowchart LR
    A1([He thong don hang\nDoanh so, so luong ban]) --> P
    A2([CRM\nSDT khach hang, pheu tu van]) --> P
    A3([Pancake\nTin nhan, page, hoi thoai]) --> P
    A4([Google Ads\nSpend, Impression, Click]) --> P
    A5([Google Sheet\nKPI nhan vien]) --> P

    P([Python\nCall API & Clean Data]) --> S([SQL Server\nData Modeling])
    S --> D([Power BI\nDashboard])

    Dagster([Dagster\nSchedule & Orchestrate]) -.-> P

    style A1 fill:#f7d794,stroke:#e6b84a
    style A2 fill:#f7d794,stroke:#e6b84a
    style A3 fill:#f7d794,stroke:#e6b84a
    style A4 fill:#f7d794,stroke:#e6b84a
    style A5 fill:#f7d794,stroke:#e6b84a
    style P fill:#a8d5c2,stroke:#5aab8e
    style S fill:#c9b8e8,stroke:#9b7fd4
    style D fill:#f4a9a8,stroke:#e07b7b
    style Dagster fill:#b8d4f4,stroke:#5a8ab8
```

| Tầng | Vai trò |
|---|---|
| Hệ thống đơn hàng | Doanh số, số lượng bán |
| CRM | SĐT khách hàng, phễu tư vấn → chốt đơn |
| Pancake | Tin nhắn, page, hội thoại |
| Google Ads | Spend, Impression, Click, Conversion |
| Google Sheet | KPI nhân viên |
| Python | Call API từng nguồn, làm sạch data và load vào SQL Server |
| Dagster | Schedule chạy tự động, monitor từng job, retry khi có lỗi |
| SQL Server | Lưu trữ, modeling, tính toán các metrics |
| Power BI | Trực quan hóa, dashboard, time intelligence |

---

## Lưu ý về dữ liệu

Do yêu cầu bảo mật, dữ liệu trong dashboard này đã được xuất ra file Excel và thay thế bằng fake data. Các con số, tên nhân viên, tên page, tỉnh thành hiển thị trên dashboard không phản ánh số liệu thực tế — chỉ mang tính minh họa cho cấu trúc và logic phân tích.

---

## Cấu trúc Dashboard

```
Homepage
   ├── Overview                  → Xem tổng quan xu hướng
   ├── Performance Summary       → Chẩn đoán nguyên nhân
   ├── Target vs. Actual         → Theo dõi mục tiêu
   ├── Sales Process Efficiency  → Phân tích phễu tư vấn
   └── Ads Performance           → Phân tích quảng cáo
```

---

## Hướng Dẫn Đọc Dashboard

### Bước 1 — Overview

Trang này trả lời câu hỏi đầu tiên: **Doanh số tăng/giảm ở thời gian nào, ở đâu?**

- Xem xu hướng doanh số theo thời gian
- Tỷ lệ đóng góp doanh số theo Source / Page / City
- So sánh với Last Month / Quarter / Week / Year

Ví dụ: Doanh số tháng 5 giảm 62% so với tháng 4 → lọc theo City thấy California giảm mạnh nhất (-70%) → lọc theo Source thấy Facebook Ads chiếm 80% doanh số ở khu vực này → cần kiểm tra hiệu quả Facebook Ads tại thị trường California.

Dùng các bộ lọc Time / Source / Page / Salesman / City để thu hẹp phạm vi trước khi đi sâu.

---

### Bước 2 — Performance Summary

Khi đã thấy doanh số giảm ở Overview, chuyển sang trang này để tìm nguyên nhân gốc rễ.

Doanh số giảm do ASP giảm hay Số đơn giảm?

**Nếu ASP giảm:**
- Giá bán có bị giảm không?
- Hành vi mua hàng có thay đổi không? Trước đây khách mua nhiều sản phẩm/đơn với giá cao, hiện tại chuyển sang mua sản phẩm giá thấp hơn → ASP kéo xuống dù số đơn không đổi

**Nếu Số đơn giảm:**
- SĐT lấy được có nhiều không?
  - SĐT nhiều nhưng tỷ lệ ra đơn (Đơn/SĐT) thấp → team có khách để nói chuyện nhưng không chốt được → vấn đề ở bước tư vấn cuối → xem tiếp Bước 3a
  - SĐT thấp → Lead có nhiều không?
    - Lead nhiều nhưng SĐT/Lead thấp → team không lấy được SĐT từ khách → vấn đề ở bước tiếp cận ban đầu → xem tiếp Bước 3a
    - Lead thấp → đầu vào quảng cáo đang yếu → kiểm tra Ads Spend còn ổn không?
      - Ads Spend giảm → ngân sách bị cắt → review ad budget → xem tiếp Bước 3b
      - Ads Spend ổn định nhưng Lead vẫn thấp → kiểm tra Impression và CTR → xem tiếp Bước 3b

```mermaid
flowchart TD
    A([Revenue down\nASP hay Quantity?]) --> B([ASP down])
    A --> C([Quantity down\nSDT co nhieu khong?])

    B --> B1([Gia ban giam?])
    B --> B2([Hanh vi mua thay doi?\nMua SP gia thap hon])
    B1 --> B3[Review pricing]
    B2 --> B3

    C --> D([SDT nhieu\nTy le ra don thap])
    C --> E([SDT thap\nLead co nhieu khong?])

    D --> D1[Buoc chot don\nReview tu van giai doan cuoi]

    E --> F([Lead nhieu\nSDT/Lead thap])
    E --> G([Lead thap\nCheck Ads])

    F --> F1[Buoc lay SDT\nReview tu van giai doan dau]

    G --> H([Ads Spend on dinh?])
    H --> H1([Spend giam\nNgan sach bi cat])
    H --> H2([Spend on dinh\nLead van thap])

    H1 --> H3[Review ad budget]

    H2 --> I1([Impression giam\nReach dang hep lai])
    H2 --> I2([CTR giam\nAds khong thu hut])

    I1 --> J1[Review targeting]
    I2 --> J2[Review creatives]

    style A fill:#f4a9a8,stroke:#e07b7b
    style B fill:#f7d794,stroke:#e6b84a
    style C fill:#f7d794,stroke:#e6b84a
    style B1 fill:#f7d794,stroke:#e6b84a
    style B2 fill:#f7d794,stroke:#e6b84a
    style D fill:#a8d5c2,stroke:#5aab8e
    style E fill:#f4a9a8,stroke:#e07b7b
    style F fill:#a8d5c2,stroke:#5aab8e
    style G fill:#f4a9a8,stroke:#e07b7b
    style H fill:#c9b8e8,stroke:#9b7fd4
    style H2 fill:#f4a9a8,stroke:#e07b7b
    style I1 fill:#f7d794,stroke:#e6b84a
    style I2 fill:#f7d794,stroke:#e6b84a
```

Ví dụ: Revenue giảm → Quantity giảm 65% nhưng ASP tăng 9% → vấn đề không phải giá mà là số đơn đang giảm → SĐT lấy được 1,179 (giảm 28%) → Lead cũng giảm 21% → kiểm tra Ads Spend: vẫn ổn định → kiểm tra Impression giảm 13% và CTR giảm 0.92% → cả targeting lẫn creative đang kém → xem tiếp Bước 3b.

---

### Bước 3a — Sales Process Efficiency

Vào trang này khi sơ đồ chỉ ra vấn đề ở khâu tư vấn.

Phễu chuyển đổi tổng quan:

```
Lead → SĐT → Tư vấn → Thương lượng → Mua hàng
100%   ~55%   ~44%      ~31%           ~13%
```

- Nhân viên nào đang có % SĐT/Lead thấp? → Chưa tiếp cận được khách
- Nhân viên nào đang có % Mua hàng thấp? → Đang mất đơn ở bước thương lượng
- Xem Lý do Fail → Khách từ chối vì Giá / Dịch vụ / Số lượng / Không phản hồi?

Ví dụ: Mr. B có % SĐT/Lead = 36%, thấp hơn trung bình 55% → có lead về nhưng không tiếp cận được khách → kiểm tra Lý do Fail: 84 case "No Response" → khách không phản hồi nhiều → cần review lại tốc độ và cách tiếp cận ban đầu.

So sánh giữa các nhân viên cùng level để xác định ai đang làm tốt, ai cần hỗ trợ.

---

### Bước 3b — Ads Performance

Vào trang này khi sơ đồ chỉ ra vấn đề ở quảng cáo.

| Câu hỏi | Chỉ số cần xem |
|---|---|
| Có đang chi đủ ngân sách không? | Ads Spend vs. Ads Budget |
| Quảng cáo có đang sinh lời không? | ROAS |
| Quảng cáo có đang tiếp cận đủ người không? | Impression |
| Người dùng có đang nhấp vào không? | CTR |
| Lượt nhấp có chuyển thành lead không? | Conversion |

So sánh Facebook vs. Google để xác định platform nào đang hiệu quả hơn. Xem theo thời gian để xác định hiệu suất bắt đầu giảm từ ngày nào.

Ví dụ: Lead giảm 21% trong khi Ads Spend vẫn ổn định → kiểm tra Impression: giảm 13%, reach đang hẹp lại → kiểm tra CTR: 0.97%, giảm 0.92% so với kỳ trước → cả targeting lẫn creative đang kém hiệu quả → cần review cả hai.

---

### Bước 4 — Target vs. Actual

Xem tiến độ đạt KPI theo 3 góc:

| Góc nhìn | Câu hỏi |
|---|---|
| Vs Full Month | Với tốc độ này, có đạt mục tiêu cả tháng không? |
| Vs Month-To-Date | Đến hôm nay, đang đạt bao nhiêu % so với kế hoạch MTD? |
| Vs Today | Hôm nay có đạt chỉ tiêu ngày không? |

Ví dụ: Ngày 20/5, % Achieved (Full Month) = 28.68% nhưng % Achieved (MTD) = 42.18% → tốc độ hiện tại chỉ đạt ~28% mục tiêu cả tháng, cần tăng tốc đáng kể → nhìn vào Detail: Mr. K đạt 89.99% MTD target đang tốt, Mr. H chỉ đạt 73.37% cần follow up.

---

## Công Nghệ Sử Dụng

- Python — Call API, làm sạch data và load vào SQL Server
- Dagster — Schedule tự động, orchestrate và monitor pipeline
- SQL Server — Lưu trữ, modeling, tính toán metrics
- Power BI — Trực quan hóa & báo cáo
- DAX — Measures, KPI, time intelligence

---
