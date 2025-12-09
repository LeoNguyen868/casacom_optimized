# **Báo cáo Nghiên cứu Kỹ thuật Chuyên sâu: Kiến trúc Xử lý Dữ liệu Geospatial Quy mô Lớn và Chiến lược Tối ưu hóa trên ClickHouse**

## **1\. Tổng quan và Phạm vi Nghiên cứu**

### **1.1. Bối cảnh Bài toán**

Trong bối cảnh bùng nổ của dữ liệu lớn (Big Data) và Internet vạn vật (IoT), việc quản lý và phân tích dữ liệu định vị (geospatial data) từ các thiết bị di động đã trở thành một thách thức kỹ thuật trọng yếu. Bài toán đặt ra liên quan đến việc xử lý dữ liệu từ hàng trăm triệu thiết bị (được định danh qua MAID \- Mobile Advertising ID) với khối lượng dữ liệu thô có thể lên tới vài Terabyte (TB) mỗi ngày. Dữ liệu này không chỉ lớn về mặt dung lượng mà còn phức tạp về mặt cấu trúc thời gian (temporal structure), đòi hỏi khả năng xử lý thời gian thực (real-time processing) để chuyển đổi từ các bản ghi thô thành các chỉ số hành vi có ý nghĩa (behavioral metrics).

Báo cáo này được xây dựng nhằm cung cấp một phân tích toàn diện và sâu sắc về kiến trúc hệ thống xử lý dữ liệu, tập trung vào việc ứng dụng cơ sở dữ liệu ClickHouse. Phạm vi nghiên cứu bao gồm việc phân tích mẫu dữ liệu đầu vào (maid\_sample.csv), giải mã logic xử lý nghiệp vụ hiện tại (evidence\_pipeline\_new.py), nghiên cứu các khả năng kỹ thuật của ClickHouse, và đánh giá tính khả thi của ba mô hình luồng dữ liệu (Data Flows) khác nhau. Mục tiêu cuối cùng là đề xuất một kiến trúc tối ưu đảm bảo hiệu năng cao, khả năng mở rộng (scalability) và chi phí hợp lý.

### **1.2. Mục tiêu Phân tích**

Báo cáo sẽ giải quyết các vấn đề cốt lõi sau:

* **Đặc tả dữ liệu:** Hiểu rõ bản chất thống kê và hành vi của dữ liệu đầu vào để thiết kế lược đồ lưu trữ tối ưu.  
* **Chuyển đổi logic:** Tái cấu trúc logic xử lý từ mô hình tuần tự (procedural) sang mô hình tập hợp (set-based/vectorized) của SQL.  
* **Tối ưu hóa ClickHouse:** Ứng dụng các kỹ thuật nén, đánh chỉ mục và các hàm tổng hợp nâng cao.  
* **Đánh giá kiến trúc:** So sánh định lượng và định tính giữa các phương án triển khai luồng dữ liệu.

## ---

**2\. Phân tích Chuyên sâu Dữ liệu Nguồn (maid\_sample.csv)**

Dữ liệu đầu vào là nền tảng của mọi quyết định kiến trúc. Phân tích chi tiết file maid\_sample.csv 1 cho thấy đây là dữ liệu chuỗi thời gian (time-series) gắn liền với không gian địa lý, mang những đặc điểm kỹ thuật đặc thù ảnh hưởng trực tiếp đến hiệu năng hệ thống.

### **2.1. Cấu trúc và Định dạng Dữ liệu**

File CSV chứa khoảng 7.000 dòng dữ liệu mô tả hành trình của một thiết bị MAID duy nhất. Các trường dữ liệu bao gồm:

* **maid (Mobile Advertising ID):** Chuỗi ký tự định danh thiết bị (ví dụ: HEHyPZs7dFuBcXKO...).  
  * *Phân tích:* Với yêu cầu hệ thống xử lý "hàng trăm triệu maid", trường này có độ phân tán (cardinality) cực cao. Trong cơ sở dữ liệu cột như ClickHouse, các trường có độ phân tán cao thường gây áp lực lớn lên bộ nhớ khi thực hiện các thao tác GROUP BY. Tuy nhiên, trong phạm vi một phân vùng dữ liệu (partition) theo thời gian, số lượng MAID hoạt động có thể thấp hơn tổng số MAID toàn hệ thống.2  
* **timestamp:** Thời gian ghi nhận sự kiện (ví dụ: 2025-08-01 18:00:34+07:00).  
  * *Phân tích:* Dữ liệu thời gian có độ chính xác đến giây (hoặc mili-giây tùy vào nguồn phát). Quan sát dữ liệu mẫu cho thấy các sự kiện không phân bố đều. Có những khoảng thời gian dày đặc sự kiện (bursts) và những khoảng trống lớn (gaps). Đây là đặc trưng của dữ liệu hành vi người dùng: thiết bị chỉ gửi tín hiệu khi có hoạt động hoặc thay đổi vị trí đáng kể.1  
* **latitude / longitude:** Tọa độ địa lý (ví dụ: 32.86305236816406, \-6.5725274085998535).  
  * *Phân tích:* Các giá trị này là số thực dấu phẩy động (Float64). Độ chính xác của phần thập phân rất cao, cho thấy dữ liệu có thể được thu thập từ GPS chất lượng cao. Tuy nhiên, sự thay đổi giữa các dòng liên tiếp thường rất nhỏ (người dùng di chuyển chậm hoặc đứng yên), tạo điều kiện thuận lợi cho các thuật toán nén chuyên dụng như Gorilla.3  
* **flux:** Mã trạng thái hoặc loại luồng (ví dụ: 'B').  
  * *Phân tích:* Đây là trường có độ phân tán thấp (Low Cardinality), rất thích hợp để mã hóa dạng từ điển (Dictionary Encoding) nhằm tiết kiệm không gian lưu trữ.  
* **geohash:** Mã hóa vị trí địa lý (ví dụ: evg6fqw).  
  * *Phân tích:* Geohash trong mẫu có độ dài 7 ký tự. Theo tiêu chuẩn Geohash, độ dài 7 ký tự tương ứng với ô lưới có kích thước khoảng 153m x 153m. Việc tính toán Geohash có thể được thực hiện ngay tại lớp ứng dụng hoặc trong cơ sở dữ liệu.5

### **2.2. Đặc điểm Hành vi và Phân bố Thống kê (Statistical Distribution)**

Phân tích sâu hơn vào các giá trị trong maid\_sample.csv 1 hé lộ những "insight" quan trọng cho việc thiết kế thuật toán:

#### **2.2.1. Hiện tượng "Burstiness" và "Silence"**

Dữ liệu cho thấy các cụm thời gian (clusters) hoạt động mạnh mẽ xen kẽ với các khoảng im lặng.

* **Cụm hoạt động:** Ví dụ các mốc thời gian 18:24:46, 18:24:47, 18:24:51, 18:24:53 trong ngày 2025-08-01. Trong vòng chưa đầy 10 giây, thiết bị gửi 4 tín hiệu. Điều này cho thấy tần suất lấy mẫu (sampling rate) có thể rất cao khi thiết bị đang hoạt động tích cực hoặc đang di chuyển.  
* **Khoảng lặng (Gaps):** Sau cụm 18:24:53, tín hiệu tiếp theo xuất hiện lúc 21:09:20, tạo ra một khoảng trống (gap) gần 3 giờ đồng hồ.  
* *Hệ quả:* Các thuật toán phân tích phiên (sessionization) hoặc tính toán thời gian lưu trú (dwell time) không thể sử dụng các hàm cửa sổ (window functions) đơn giản với giả định dữ liệu liên tục. Cần phải sử dụng các kỹ thuật "Gap Analysis" để xác định và xử lý các khoảng trống này.6

#### **2.2.2. Sự trôi dạt GPS (GPS Drift) và Geohash**

Quan sát các dòng dữ liệu liên tiếp, ta thấy geohash thay đổi từ evg6fqw sang evg6fqt và ngược lại, dù thời gian chỉ cách nhau vài giây. Điều này phản ánh hiện tượng nhiễu GPS hoặc thiết bị nằm ở ranh giới giữa các ô lưới geohash.

* *Hệ quả:* Việc sử dụng geohash thô để đếm số lượng địa điểm duy nhất (unique\_days hoặc pings per geohash) có thể dẫn đến việc phóng đại số lượng địa điểm thực tế. Cần có cơ chế làm mịn (smoothing) hoặc gộp nhóm (clustering) trước khi tổng hợp.

#### **2.2.3. Quy mô Dữ liệu và Tải trọng**

Với "hàng trăm triệu maid" và vài TB dữ liệu mỗi ngày, ta có thể ước tính:

* Mỗi dòng dữ liệu (CSV) khoảng 100-150 bytes.  
* 1 TB dữ liệu tương ứng với khoảng 7-10 tỷ bản ghi (rows) mỗi ngày.  
* Đây là quy mô "High-Volume, High-Velocity". Hệ thống lưu trữ phải hỗ trợ ghi (write) với tốc độ hàng triệu dòng/giây và nén dữ liệu cực tốt để giảm chi phí lưu trữ.7

## ---

**3\. Giải mã Logic Nghiệp vụ (evidence\_pipeline\_new.py) và Cấu trúc Dữ liệu Đích**

Để chuyển đổi hệ thống sang ClickHouse, chúng ta cần "dịch" logic xử lý từ mã lệnh (Python/Pandas) sang tư duy tập hợp của SQL và cấu trúc dữ liệu tối ưu.

### **3.1. Phân tích stored\_data\_new.json: Trạng thái Trung gian (Intermediate State)**

File stored\_data\_new.json 1 không phải là báo cáo cuối cùng mà là nơi lưu trữ **trạng thái tích lũy** của dữ liệu. Cấu trúc của nó cho thấy sự chuyển dịch từ dữ liệu dạng dòng (row-based logs) sang dữ liệu dạng cột/mảng (columnar/array-based profiles).

* **Mảng geohash và pings:** Thay vì lưu mỗi lần ping là một dòng, hệ thống lưu danh sách các geohash duy nhất mà thiết bị đã ghé qua, kèm theo số lượng ping tại mỗi geohash. Đây là dạng cấu trúc Map\<String, Count\> hoặc hai mảng song song Array(String) và Array(Int).  
* **Mảng timestamps (ẩn ý trong Gap Analysis):** Để tính toán các trường gap\_bins\_0d, gap\_bins\_1\_3d..., hệ thống cần giữ lại thông tin về thời điểm xuất hiện. Trong stored\_data\_new, các giá trị này đã được tính toán thành histogram. Tuy nhiên, để cập nhật (incremental update) các bin này khi có dữ liệu mới, ta cần biết thời điểm cuối cùng (last\_seen) của lần xử lý trước.  
* **Các chỉ số thống kê (mean\_lat, mean\_lon, std\_geohash\_m):** Đây là các đại lượng thống kê mô tả (descriptive statistics) giúp định vị tâm hoạt động và độ phân tán của thiết bị.

*Ý nghĩa kỹ thuật:* stored\_data\_new.json đại diện cho mô hình **Pre-aggregation**. Trong ClickHouse, mô hình này tương ứng hoàn hảo với AggregatingMergeTree, nơi các trạng thái trung gian (intermediate aggregation states) được lưu trữ dưới dạng nhị phân thay vì JSON text.8

### **3.2. Phân tích aggregrated\_data\_new.json: Chỉ số Phân tích Nâng cao**

File aggregrated\_data\_new.json 1 chứa các chỉ số phức tạp hơn, được dẫn xuất từ stored\_data hoặc tính toán lại từ đầu.

#### **3.2.1. Entropy Chuẩn hóa (entropy\_hour\_norm)**

Đây là chỉ số đo lường tính bất định (randomness) trong hành vi thời gian của người dùng.

* **Cơ sở lý thuyết:** Sử dụng Shannon Entropy: $H(X) \= \- \\sum\_{i=1}^{n} P(x\_i) \\log\_2 P(x\_i)$.  
* **Ứng dụng:** Với $X$ là phân phối xác suất xuất hiện trong các khung giờ (0h-23h). Nếu người dùng chỉ xuất hiện vào 9h sáng hàng ngày, Entropy $\\approx 0$. Nếu xuất hiện đều đặn mọi giờ, Entropy đạt cực đại.  
* **Chuẩn hóa:** Giá trị được chia cho $\\log\_2(24)$ để đưa về thang đo .  
* *Thách thức trong ClickHouse:* ClickHouse có hàm entropy(column), nhưng để tính entropy trên phân phối giờ của *từng* người dùng, ta cần áp dụng hàm này trên mảng (Array) hoặc sử dụng arrayMap và arraySum để tự tính toán.9

#### **3.2.2. Độ ổn định (Stability) và Các Tỷ lệ (Ratios)**

* **monthly\_stability:** Đo lường sự biến thiên số ngày hoạt động giữa các tháng. Yêu cầu tính phương sai (variance) hoặc độ lệch chuẩn (stddev) của chuỗi số liệu theo tháng.10  
* **night\_ratio, weekend\_ratio:** Tỷ lệ phần trăm hoạt động vào ban đêm hoặc cuối tuần. Đây là các phép tính đếm có điều kiện (Conditional Count). Trong ClickHouse, chúng được thực hiện hiệu quả bằng hàm countIf hoặc sumIf.11

#### **3.2.3. Phân tích Khoảng trống (Gap Analysis)**

Các trường gap\_bins trong stored\_data yêu cầu tính hiệu số thời gian giữa các sự kiện liên tiếp: $\\Delta t\_i \= t\_i \- t\_{i-1}$.

* Trong Python, việc này thường dùng vòng lặp hoặc df.diff().  
* Trong ClickHouse, với dữ liệu phân tán, việc này đòi hỏi kỹ thuật sử dụng hàm arrayDifference trên mảng thời gian đã được sắp xếp (arraySort).12

## ---

**4\. Nghiên cứu Nền tảng Công nghệ: ClickHouse Deep Dive**

Để hiện thực hóa logic trên với quy mô TB dữ liệu, ClickHouse cung cấp một hệ sinh thái các tính năng mạnh mẽ. Phần này sẽ đi sâu vào các cơ chế nội tại của ClickHouse phù hợp với bài toán.

### **4.1. Cơ chế Lưu trữ và Nén (Storage & Compression)**

Dữ liệu geospatial time-series có tính chất lặp lại cao, cho phép nén cực kỳ hiệu quả.

* **Timestamp (DateTime64):** Dữ liệu thời gian tăng dần đơn điệu. Sử dụng codec DoubleDelta hoặc Delta sẽ lưu trữ sự chênh lệch giữa các giá trị (thường là hằng số hoặc số nhỏ), thay vì lưu toàn bộ 8 bytes của timestamp. Kết hợp với LZ4 hoặc ZSTD, tỷ lệ nén có thể đạt mức rất cao.  
  * *Cấu hình đề xuất:* CODEC(Delta(4), ZSTD(1)).13  
* **Latitude/Longitude (Float64):** Tọa độ GPS thường thay đổi rất ít giữa các điểm liên tiếp. Codec Gorilla được thiết kế riêng cho dữ liệu dấu phẩy động dạng chuỗi thời gian, sử dụng thuật toán XOR để nén phần định trị (mantissa) và phần mũ (exponent).  
  * *Cấu hình đề xuất:* CODEC(Gorilla, ZSTD(1)).3  
* **Maid (String):** Với cardinality cao, LowCardinality có thể không hiệu quả và gây tốn RAM. Nên sử dụng ZSTD cho cột String. Nếu cần tối ưu hóa tốc độ GROUP BY, có thể cân nhắc hash chuỗi maid sang UInt64 (ví dụ dùng sipHash64), chấp nhận rủi ro va chạm nhỏ (collision) để đổi lấy tốc độ và kích thước nhỏ gọn.

### **4.2. Table Engines: Trái tim của Hệ thống**

* **MergeTree:** Engine cơ bản để lưu dữ liệu Raw. Cơ chế "LSM-tree like" của nó tối ưu cho việc ghi dữ liệu (append-only) tốc độ cao. Dữ liệu được sắp xếp theo khóa chính (ORDER BY), cho phép truy vấn khoảng (range query) cực nhanh.14  
* **AggregatingMergeTree:** Đây là chìa khóa để thay thế stored\_data\_new.json. Engine này cho phép lưu trữ trạng thái của các hàm tổng hợp (AggregateFunction). Khi các phần dữ liệu (data parts) được trộn (merge) lại với nhau trong nền, ClickHouse sẽ tự động kết hợp các trạng thái này.  
  * *Ví dụ:* Trạng thái uniqState(geohash) của ngày hôm nay sẽ được merge với uniqState(geohash) của ngày hôm qua để ra kết quả uniq tổng thể mà không cần đếm lại từ đầu.8

### **4.3. Các Hàm Mảng và Tổng hợp Nâng cao (Advanced Array Functions)**

ClickHouse vượt trội so với các DB khác nhờ khả năng xử lý mảng mạnh mẽ, cho phép thực hiện logic phức tạp ngay trong SQL.

* **groupArray & arraySort:** Dùng để gom nhóm toàn bộ timestamp của một user thành một mảng và sắp xếp chúng. Đây là bước tiền đề cho Gap Analysis.16  
* **arrayDifference:** Hàm vector hóa tính hiệu số giữa các phần tử liền kề: $Y\_i \= X\_i \- X\_{i-1}$. Đây chính là công cụ để tính khoảng cách thời gian giữa các lần ping.12  
* **arrayFilter & arrayCount:** Sau khi có mảng các khoảng cách (gaps), ta dùng các hàm này để đếm số lượng gap thuộc các bin khác nhau (ví dụ: x \> 86400).12  
* **Bitwise Operations (groupBitOr):** Để theo dõi ngày hoạt động (unique\_days), thay vì lưu một mảng danh sách ngày (tốn bộ nhớ), ta có thể dùng một số nguyên (UInt32 hoặc UInt64) làm bitmask. Mỗi bit tương ứng với một ngày trong tháng. Phép toán groupBitOr cho phép hợp nhất lịch sử hoạt động cực nhanh.17

## ---

**5\. Phân tích Khả thi của Ba Luồng Tổng hợp (Feasibility Analysis)**

Dựa trên yêu cầu, chúng ta xem xét 3 luồng xử lý. Phân tích này dựa trên các tiêu chí: Hiệu năng (Latency/Throughput), Chi phí (Storage/Compute), và Độ phức tạp vận hành (Operational Complexity).

### **5.1. Luồng 1: Lưu trữ Raw \-\> Tổng hợp ra stored\_data\_new (Batch Processing)**

*Mô tả:* Dữ liệu Raw được lưu vào ClickHouse (hoặc S3/HDFS). Định kỳ (ví dụ: hàng ngày), một hệ thống ngoài (Spark/Python) đọc dữ liệu, tính toán logic phức tạp, và ghi kết quả stored\_data lại vào ClickHouse hoặc file system.

* **Ưu điểm:**  
  * Logic xử lý được viết bằng ngôn ngữ lập trình đa năng (Python/Java), dễ dàng xử lý các thuật toán đệ quy hoặc logic nghiệp vụ cực kỳ phức tạp mà SQL khó diễn đạt.  
  * Tách biệt tải tính toán (Compute) khỏi tải truy vấn (Query) của DB.  
* **Nhược điểm (Vấn đề quy mô lớn):**  
  * **I/O Bottleneck:** Với vài TB dữ liệu mỗi ngày, việc đọc toàn bộ dữ liệu ra khỏi DB để xử lý rồi ghi lại là một sự lãng phí tài nguyên mạng và đĩa khủng khiếp.  
  * **Độ trễ cao (High Latency):** Dữ liệu stored\_data thường chỉ có sẵn sau khi job chạy xong (thường là T+1 ngày). Không đáp ứng được nhu cầu real-time.  
  * **State Management:** Để tính gap\_bins chính xác, job ngày hôm nay phải biết trạng thái last\_seen của ngày hôm qua. Việc quản lý trạng thái này giữa các lần chạy batch rất phức tạp và dễ lỗi.1  
* **Đánh giá:** Thấp. Không phù hợp cho quy mô TB/ngày nếu muốn tối ưu chi phí và thời gian.

### **5.2. Luồng 2: Lưu trữ stored\_data\_new \-\> Tổng hợp ra aggregated\_data\_new**

*Mô tả:* Dữ liệu Raw được chuyển đổi thành stored\_data (dạng AggregatingMergeTree) ngay khi nạp vào. Khi cần báo cáo, ta truy vấn từ bảng stored\_data để ra aggregated\_data.

* **Ưu điểm:**  
  * **Incremental Aggregation:** ClickHouse tự động cập nhật stored\_data. Dữ liệu mới được merge vào dữ liệu cũ một cách tự động.  
  * **Giảm dung lượng lưu trữ:** Bảng stored\_data nhỏ hơn bảng Raw từ 10-100 lần (tùy tỷ lệ nén).  
  * **Linh hoạt:** Từ stored\_data, có thể sinh ra nhiều loại báo cáo khác nhau (aggregated\_data) mà không cần truy cập lại Raw.  
* **Nhược điểm:**  
  * **Độ phức tạp schema:** Cần thiết kế bảng AggregatingMergeTree chính xác với các kiểu dữ liệu AggregateFunction.  
  * **Vấn đề kích thước mảng:** Nếu một MAID có quá nhiều pings (ví dụ: bot), mảng timestamps lưu trong một dòng có thể vượt quá giới hạn bộ nhớ của một block, gây lỗi khi merge. Cần kỹ thuật "bucketing" hoặc giới hạn kích thước mảng.8  
* **Đánh giá:** Trung bình \- Cao. Đây là mô hình "Rollup" kinh điển.

### **5.3. Luồng 3: Lưu trữ Raw \-\> Tổng hợp ra aggregated\_data\_new (Real-time Materialization)**

*Mô tả:* Đây là phương pháp tiếp cận hiện đại nhất với ClickHouse. Dữ liệu được nạp vào bảng Raw (có thể là bảng tạm hoặc bảng có TTL ngắn). Một hoặc nhiều **Materialized Views (MV)** sẽ kích hoạt ngay khi có dữ liệu mới chèn vào (On-Insert Trigger), thực hiện tính toán logic và cập nhật trực tiếp vào bảng đích (có thể là bảng aggregated hoặc bảng state được tối ưu hóa).

* **Cơ chế:**  
  1. INSERT vào bảng Raw.  
  2. MV kích hoạt: Tính Geohash, Map timestamp vào các bucket, tính toán các biến đếm sơ bộ.  
  3. Kết quả được đẩy vào bảng đích (Target Table) dưới dạng SummingMergeTree hoặc AggregatingMergeTree.  
  4. Truy vấn aggregated\_data chỉ việc đọc từ bảng đích đã được tính toán sẵn.  
* **Ưu điểm vượt trội:**  
  * **Real-time:** Các chỉ số được cập nhật gần như ngay lập tức (sub-second).  
  * **Hiệu quả chi phí:** Chỉ tính toán trên phần dữ liệu *mới* (incremental data). Không cần quét lại dữ liệu cũ.  
  * **Đơn giản hóa Pipeline:** Loại bỏ sự cần thiết của hệ thống ETL bên ngoài (Airflow/Spark) cho các tác vụ cơ bản.  
* **Thách thức:**  
  * Logic tính toán phức tạp (như Entropy hay Gap Analysis trên toàn bộ lịch sử) khó thực hiện hoàn toàn trong MV vì MV chỉ nhìn thấy block dữ liệu đang được chèn vào (stateless processing trên block). Tuy nhiên, ta có thể kết hợp MV để tạo ra các "state" trung gian (như Luồng 2\) và thực hiện bước tổng hợp cuối cùng khi truy vấn (Query-time aggregation).  
* **Kết luận:** **Luồng 3 kết hợp Luồng 2 (Modified Flow 3\)** là phương án tối ưu nhất. Sử dụng MV để chuyển dữ liệu Raw thành các trạng thái trung gian (stored\_data trong AggregatingMergeTree), sau đó truy vấn để ra aggregated\_data.

## ---

**6\. Thiết kế Giải pháp Chi tiết (Detailed Implementation Design)**

Dựa trên kết luận chọn Luồng 3 (Modified), dưới đây là thiết kế chi tiết các thành phần trong ClickHouse.

### **6.1. Lược đồ Cơ sở dữ liệu (Database Schema)**

#### **6.1.1. Bảng Raw (Lớp Ingestion)**

Bảng này tiếp nhận dữ liệu đầu vào. Thiết lập TTL để tự động xóa dữ liệu sau 7 ngày nhằm tiết kiệm chi phí lưu trữ, vì dữ liệu quan trọng đã được chuyển sang bảng Aggregated.

SQL

CREATE TABLE raw\_maid\_pings (  
    maid String,   
    timestamp DateTime64(3) CODEC(Delta(4), ZSTD(1)), \-- Tối ưu nén thời gian  
    latitude Float64 CODEC(Gorilla, ZSTD(1)),         \-- Tối ưu nén tọa độ  
    longitude Float64 CODEC(Gorilla, ZSTD(1)),  
    flux LowCardinality(String),  
    \-- Geohash được tính toán tự động (Materialized Column) để giảm tải CPU khi query  
    geohash String MATERIALIZED geohashEncode(longitude, latitude, 7)   
) ENGINE \= MergeTree()  
PARTITION BY toDate(timestamp) \-- Phân vùng theo ngày  
ORDER BY (maid, timestamp)     \-- Sắp xếp tối ưu cho truy vấn chuỗi thời gian  
TTL timestamp \+ INTERVAL 7 DAY;

#### **6.1.2. Bảng Trạng thái (Intermediate State Table \- Thay thế stored\_data\_new.json)**

Bảng này sử dụng AggregatingMergeTree để lưu trữ các trạng thái tính toán.

SQL

CREATE TABLE maid\_state\_agg (  
    maid String,  
      
    \-- Trạng thái cho các chỉ số đếm cơ bản  
    total\_pings SimpleAggregateFunction(sum, UInt64),  
    first\_seen SimpleAggregateFunction(min, DateTime64(3)),  
    last\_seen SimpleAggregateFunction(max, DateTime64(3)),  
      
    \-- Lưu mảng thời gian để phục vụ tính Gap và Entropy sau này  
    \-- Sử dụng groupArrayState để gom nhóm timestamp  
    time\_points\_state AggregateFunction(groupArray, DateTime64(3)),  
      
    \-- Lưu danh sách geohash và số lượng ping tương ứng  
    \-- Map\<Geohash, Count\>  
    geohash\_map\_state AggregateFunction(sumMap, Map(String, UInt64)),  
      
    \-- Tọa độ tổng (dùng để tính trung bình)  
    sum\_lat SimpleAggregateFunction(sum, Float64),  
    sum\_lon SimpleAggregateFunction(sum, Float64)  
) ENGINE \= AggregatingMergeTree()  
ORDER BY maid; \-- Sắp xếp theo maid để gom dữ liệu

### **6.2. Chiến lược Materialized View (Thay thế Python Pipeline)**

MV đóng vai trò như một "worker" liên tục chuyển đổi dữ liệu.

SQL

CREATE MATERIALIZED VIEW mv\_raw\_to\_agg TO maid\_state\_agg AS  
SELECT  
    maid,  
    count() AS total\_pings,  
    min(timestamp) AS first\_seen,  
    max(timestamp) AS last\_seen,  
      
    \-- Tạo trạng thái mảng thời gian  
    groupArrayState(timestamp) AS time\_points\_state,  
      
    \-- Tạo map geohash: {geohash: 1} cho mỗi dòng, sau đó sumMap sẽ cộng dồn  
    sumMapState(map(geohash, 1)) AS geohash\_map\_state,  
      
    sum(latitude) AS sum\_lat,  
    sum(longitude) AS sum\_lon  
FROM raw\_maid\_pings  
GROUP BY maid;

### **6.3. Truy vấn Tổng hợp Logic Phức tạp (Gap & Entropy Logic)**

Đây là phần quan trọng nhất: chuyển đổi logic Python trong evidence\_pipeline\_new.py sang SQL. Truy vấn này được thực hiện trên bảng maid\_state\_agg. Khi chạy, ClickHouse sẽ merge các trạng thái lại và trả về kết quả.

#### **6.3.1. Kỹ thuật tính Gap Analysis bằng SQL**

Logic: Lấy mảng timestamp \-\> Sắp xếp \-\> Tính arrayDifference \-\> Phân loại vào các bins.

SQL

SELECT  
    maid,  
    \-- Giải nén trạng thái timestamp  
    groupArrayMerge(time\_points\_state) AS all\_timestamps,  
    \-- Sắp xếp  
    arraySort(all\_timestamps) AS sorted\_ts,  
    \-- Tính khoảng cách (Gap) bằng giây  
    arrayDifference(sorted\_ts) AS gaps,  
      
    \-- Tính Gap Bins (ví dụ logic)  
    countEqual(arrayMap(g \-\> g \= 0, gaps), 1) AS gap\_0d\_count,  
    countEqual(arrayMap(g \-\> g \> 0 AND g \<= 86400\*3, gaps), 1) AS gap\_1\_3d\_count,  
    countEqual(arrayMap(g \-\> g \> 86400\*3 AND g \<= 86400\*7, gaps), 1) AS gap\_4\_7d\_count  
      
FROM maid\_state\_agg  
GROUP BY maid

#### **6.3.2. Kỹ thuật tính Entropy Chuẩn hóa bằng SQL**

Logic: Lấy giờ từ timestamp \-\> Đếm tần suất mỗi giờ \-\> Tính xác suất $p$ \-\> Tính công thức Entropy.

SQL

WITH   
    \-- Tính mảng các giờ (0-23)  
    arrayMap(t \-\> toHour(t), sorted\_ts) AS hours,  
    \-- Đếm số lần xuất hiện của mỗi giờ (histogram)  
    arrayMap(h \-\> countEqual(hours, h), range(24)) AS hour\_counts,  
    \-- Tổng số ping  
    arraySum(hour\_counts) AS total,  
    \-- Tính xác suất p (tránh chia cho 0\)  
    arrayMap(c \-\> if(c\=0, 0, c/total), hour\_counts) AS probs,  
    \-- Tính Entropy: \-sum(p \* log2(p))  
    \-1 \* arraySum(arrayMap(p \-\> if(p\=0, 0, p \* log2(p)), probs)) AS entropy\_val  
SELECT   
    maid,  
    \-- Chuẩn hóa entropy  
    entropy\_val / log2(24) AS entropy\_hour\_norm  
FROM...

*(Lưu ý: Đoạn mã SQL trên là logic minh họa. Trong thực tế, ClickHouse hỗ trợ các hàm tối ưu hơn hoặc có thể viết User Defined Function (UDF) nếu logic quá dài).*

## ---

**7\. Các Thách thức Tiềm ẩn và Chiến lược Giảm thiểu**

Khi triển khai kiến trúc này ở quy mô hàng trăm triệu MAID, một số vấn đề kỹ thuật có thể phát sinh:

### **7.1. Vấn đề "Array Explosion" (Bùng nổ kích thước mảng)**

* *Vấn đề:* Nếu một thiết bị hoạt động liên tục (ví dụ: bot hoặc thiết bị IoT gắn trên xe vận tải), mảng time\_points\_state có thể chứa hàng triệu phần tử. Việc load mảng này vào RAM để tính arraySort và arrayDifference có thể gây lỗi Out-Of-Memory (OOM) hoặc làm chậm truy vấn đáng kể.16  
* *Giải pháp:*  
  * **Bucketing/Sampling:** Thay vì lưu toàn bộ timestamp, chỉ lưu mẫu (sample) hoặc lưu dưới dạng histogram (ví dụ: số lượng ping trong mỗi giờ) bằng cách sử dụng AggregateFunction(histogram,...).18  
  * **Intermediate Gap Calculation:** Thay vì tính gap ở bước cuối cùng, hãy tính gap sơ bộ trong Materialized View. Tuy nhiên, điều này khó vì MV không nhìn thấy trạng thái trước đó.  
  * **Giới hạn độ dài:** Sử dụng groupArraySample để chỉ giữ lại N timestamp đại diện mới nhất.

### **7.2. Hiệu năng Truy vấn trên Cardinality Cao**

* *Vấn đề:* Truy vấn GROUP BY maid trên hàng trăm triệu thiết bị là một tác vụ nặng.  
* *Giải pháp:*  
  * **Data Skipping Indices:** Sử dụng Bloom Filter index trên cột geohash hoặc maid để tăng tốc độ lọc.19  
  * **Projections:** Tạo các Projection (một dạng view nội tại của bảng) để sắp xếp dữ liệu theo các chiều khác nhau (ví dụ: sắp xếp theo geohash thay vì maid) nhằm phục vụ các truy vấn phân tích địa lý.

### **7.3. Đồng bộ Logic Nghiệp vụ**

* *Vấn đề:* Logic tính toán nằm trong SQL của Materialized View rất khó thay đổi. Nếu muốn đổi cách tính entropy, ta phải tạo bảng mới và migrate dữ liệu.  
* *Giải pháp:* Giữ logic trong MV ở mức đơn giản nhất (chỉ gom nhóm, cộng dồn). Các logic phức tạp (như công thức entropy, ngưỡng gap) nên để ở tầng truy vấn (SELECT time) hoặc tầng ứng dụng API. Điều này giúp hệ thống linh hoạt hơn (Schema-on-Read approach).

## ---

**8\. Kết luận và Kiến nghị**

Dựa trên phân tích toàn diện, báo cáo đưa ra các kết luận sau:

1. **Chuyển đổi Mô hình:** Việc xử lý 7.000 dòng dữ liệu mẫu cho một MAID nhân lên với quy mô hàng trăm triệu MAID là bất khả thi với mô hình xử lý file truyền thống. Chuyển đổi sang **ClickHouse** với mô hình xử lý vector hóa (vectorized processing) là bắt buộc.  
2. **Kiến trúc Tối ưu:** Sử dụng **Luồng 3 (Modified)**: Raw Table (TTL ngắn) $\\to$ Materialized View $\\to$ AggregatingMergeTree (lưu trữ State) $\\to$ View (tính toán Metrics).  
   * Kiến trúc này cân bằng hoàn hảo giữa tốc độ ghi (Ingestion), chi phí lưu trữ (Storage Cost) và tốc độ truy vấn (Query Latency).  
3. **Chiến lược Lưu trữ:**  
   * Sử dụng Codec Gorilla cho tọa độ và Delta cho thời gian để tối ưu hóa nén.  
   * Sử dụng AggregatingMergeTree để thay thế hoàn toàn file JSON trung gian, giảm thiểu I/O.  
4. **Xử lý Logic Phức tạp:** Tận dụng sức mạnh của Array Functions trong ClickHouse để thực hiện Gap Analysis và Entropy Calculation ngay trong Database, loại bỏ sự phụ thuộc vào các hệ thống tính toán bên ngoài.

**Khuyến nghị triển khai:** Bắt đầu triển khai thử nghiệm (PoC) với kiến trúc Luồng 3 trên một tập dữ liệu mẫu (ví dụ: 1% lượng dữ liệu thực tế) để đo đạc hiệu năng nén và tinh chỉnh các tham số cấu hình bộ nhớ cho các hàm mảng.