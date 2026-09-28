import threading

from src.modules.rtsp_credentials import redact
from threading import Event

def process_cameras_sequential(urls, process_func, max_frames=5):
    """
    Chạy tuần tự từng camera:
    - Nếu result != None thì chuyển sang cam khác
    - Nếu qua max_frames mà vẫn None thì cũng chuyển cam
    process_func: hàm xử lý 1 frame, trả về result
    """
    for idx, url in enumerate(urls, start=1):
        print(f"[Sequential] Processing cam {idx}: {redact(url)}")
        result = None
        for frame_idx in range(max_frames):
            result = process_func(url, idx, frame_idx)
            if result is not None:
                print(f"[Sequential] Cam {idx} got result at frame {frame_idx+1}")
                break
        if result is None:
            print(f"[Sequential] Cam {idx} no result after {max_frames} frames")

def process_cameras_parallel(urls, process_func, cooldown_sec=60):
    """
    Chạy đồng thời tất cả các cam, mỗi cam 1 thread.
    Nếu result != None thì dừng lấy frame 1 phút rồi mới tiếp tục.
    process_func: hàm xử lý 1 frame, trả về result
    """
    stop_events = [Event() for _ in urls]

    def cam_worker(url, idx, stop_event):
        frame_idx = 0
        while not stop_event.is_set():
            result = process_func(url, idx, frame_idx)
            if result is not None:
                print(f"[Parallel] Cam {idx} got result at frame {frame_idx+1}, sleeping {cooldown_sec}s")
                stop_event.wait(timeout=cooldown_sec)
            frame_idx += 1

    threads = []
    for idx, (url, stop_event) in enumerate(zip(urls, stop_events), start=1):
        t = threading.Thread(target=cam_worker, args=(url, idx, stop_event), daemon=True)
        threads.append(t)
        t.start()

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("Stopping all camera threads...")
        for e in stop_events:
            e.set()
