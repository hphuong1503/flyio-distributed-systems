# Challenge 3a: Single-Node Broadcast - Đúc kết & Bài học Kỹ thuật (Summary)

> **Môi trường & Đề bài:**
> `./maelstrom test -w broadcast --bin <binary> --node-count 1 --time-limit 20 --rate 10`
> **Kết quả:** `Everything looks good! ヽ(‘ー`)ノ` (PASS)

---

## 1. Bản chất & Mục tiêu cốt lõi của Challenge 3a

* **Mục tiêu:** Xây dựng một node đơn lẻ (`n1`) có khả năng lưu trữ các giá trị broadcast từ client và trả về toàn bộ các giá trị đã lưu khi được yêu cầu đọc.
* **Ý nghĩa kiến trúc:** 3a là bước dựng móng (baseline sanity check) về **Giao thức (JSON-RPC Protocol Contract)** và **An toàn luồng (Thread-safety)** trước khi bước vào mạng phân tán đa node phức tạp (3b, 3c, 3d, 3e).

---

## 2. Các bài học kỹ thuật cốt lõi (Key Takeaways)

### 2.1. Concurrency & Cơ chế bên dưới của Maelstrom Go
* **Thực tế:** Thư viện `github.com/jepsen-io/maelstrom/demo/go` tạo ra **1 Goroutine độc lập** (`go func() { ... }`) cho mỗi tin nhắn nhận được từ `os.Stdin`.
* **Rủi ro:** Khi có nhiều request ghi (`broadcast`) và đọc (`read`) đến dồn dập cùng micro-giây, nếu truy cập vào state dùng chung mà không có cơ chế khóa, chương trình sẽ bị **Data Race** và crash (`fatal error: concurrent map read and map write`).
* **Giải pháp:** Sử dụng `sync.RWMutex` để bảo vệ dữ liệu trên HEAP.

### 2.2. Phân biệt chính xác `sync.Mutex` vs `sync.RWMutex`
* **Đọc chung (Shared - `RLock`):** Nhiều goroutine có thể cùng đọc đồng thời vì thao tác đọc không làm thay đổi hay hủy hoại bộ nhớ.
* **Ghi độc quyền (Exclusive - `Lock`):** Khi một goroutine đang ghi, **tuyệt đối không ai khác được phép đọc hoặc ghi**. Mọi goroutine khác phải xếp hàng đợi.
* **Nguyên tắc vàng:** **Tối thiểu hóa Critical Section (Minimize Lock Scope)**.
  * Chỉ giữ lock trong lúc thao tác trên RAM.
  * **Giải phóng lock trước khi thực hiện I/O** (`n.Reply` in ra stdout). Không bao giờ giữ lock trong khi đang gửi phản hồi qua mạng / I/O.

### 2.3. Cấu trúc dữ liệu: Set qua `map[int]struct{}`
* Go không có kiểu `Set` có sẵn.
* Dùng `map[int]struct{}`:
  * `struct{}` là kiểu dữ liệu rỗng có kích thước đúng **0 byte** trong bộ nhớ Go.
  * Thao tác thêm và kiểm tra trùng lặp có độ phức tạp $O(1)$.
  * Chuẩn bị sẵn khả năng chống trùng lặp (deduplication) khi bước sang 3b (nơi các node gossip lặp lại tin nhắn).
* **Tối ưu hóa Slice khi đọc:** Trong hàm `read`, sử dụng `make([]int, 0, len(s.messages))` để cấp phát trước capacity, tránh việc Go runtime phải resize mảng liên tục khi `append`.

### 2.4. Vòng đời và Vị trí bộ nhớ trong Go (Memory Lifecycle)
* **Stack vs Heap:** Biến `server` được khởi tạo bằng con trỏ `s := &server{...}` trong `main()` và được các goroutine tham chiếu tới. Trình biên dịch Go sử dụng **Escape Analysis** để đưa `s` lên vùng nhớ **HEAP**.
* Dữ liệu trong `messages` tồn tại xuyên suốt vòng đời của tiến trình, cho phép các goroutine khác nhau cùng truy cập và chia sẻ dữ liệu an toàn.

---

## 3. Kiến trúc mã nguồn mẫu chuẩn mực

```go
package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n        *maelstrom.Node
	mu       sync.RWMutex
	messages map[int]struct{}
}

type BroadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body BroadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Critical section: Thu hẹp tối đa
	s.mu.Lock()
	s.messages[body.Message] = struct{}{}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	// Khóa đọc chia sẻ
	s.mu.RLock()
	messages := make([]int, 0, len(s.messages)) // Pre-allocate capacity
	for m := range s.messages {
		messages = append(messages, m)
	}
	s.mu.RUnlock() // Mở khóa ngay sau khi copy xong, trước khi Reply I/O

	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// Ở 3a chỉ có 1 node, chỉ cần phản hồi OK
	return s.n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func main() {
	s := &server{
		n:        maelstrom.NewNode(),
		messages: make(map[int]struct{}),
	}

	s.n.Handle("broadcast", s.handleBroadcast)
	s.n.Handle("read", s.handleRead)
	s.n.Handle("topology", s.handleTopology)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
```

---

## 4. Chuẩn bị tâm thế cho Challenge 3b (Multi-Node Broadcast)

* **Thách thức mới ở 3b:**
  * Maelstrom sẽ chạy **5 nodes** (`n0, n1, n2, n3, n4`).
  * Khi client gửi `broadcast` vào `n0`, các node khác (`n1..n4`) cũng phải nhận được dữ liệu để khi client `read` ở bất kỳ node nào cũng thấy đủ messages.
* **Vũ khí mang theo từ 3a:**
  * State `map[int]struct{}` chống trùng lặp.
  * Mutex bảo vệ an toàn luồng khi nhận tin từ cả client lẫn các node bạn bè (peers).
  * Bước tiếp theo: Cài đặt cơ chế **Gossip Protocol** để các node chủ động "buôn chuyện" và truyền tin cho nhau.
