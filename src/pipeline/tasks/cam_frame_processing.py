from src.pipeline.tasks.capture_frames import capture_from_rtsp
from src.pipeline.tasks.detect_objects import detecting_objects_by_yolo
from src.pipeline.tasks.save_frames_and_labels import save_frames_and_labels
from src.pipeline.tasks.crop_objects import crop_object_cls_xyxy_nms
from src.pipeline.tasks.anpr import ocr_on_image
from src.pipeline.tasks.save_anpr_results import save_anpr_results
from src.pipeline.tasks.predict_person_attributes import predict_person_attributes
from src.modules.config import HELMET_VEHICLE_CONFIG, OUTPUT_DIR, VEHICLE_CONFIG, PERSON_CONFIG, PADDLE_OCR_CONFIG, PERSON_ATTRIBUTES_CONFIG

def process_vehicle_anpr_cam(url, idx, frame_idx):
    try:
        frame = capture_from_rtsp(url)
        if frame is None:
            print(f"[Cam {idx}] Failed to capture frame (frame_idx={frame_idx})")
            return None
        labels = detecting_objects_by_yolo(frame, VEHICLE_CONFIG)
        if labels is None:
            print(f"[Cam {idx}] Detection failed (frame_idx={frame_idx})")
            return None
        save_frames_and_labels(frame, labels, OUTPUT_DIR['vehicle'], f"cam_{idx}")
        img_crops = crop_object_cls_xyxy_nms(frame, labels, class_id=[0, 1], nms_status=False)
        if not img_crops:
            print(f"[Cam {idx}] No crops (frame_idx={frame_idx})")
            return None
        for crop in img_crops:
            ocr_result = ocr_on_image(crop, PADDLE_OCR_CONFIG)
            if ocr_result:
                save_anpr_results(crop, ocr_result, OUTPUT_DIR['anpr'], f"cam_{idx}")
                return ocr_result
        return None
    except Exception as e:
        print(f"[Cam {idx}] Error: {e}")
        return None

def process_person_attributes_cam(url, idx, frame_idx):
    try:
        frame = capture_from_rtsp(url)
        if frame is None:
            print(f"[Person Cam {idx}] Failed to capture frame (frame_idx={frame_idx})")
            return None
        labels = detecting_objects_by_yolo(frame, PERSON_CONFIG)
        if labels is None:
            print(f"[Person Cam {idx}] Detection failed (frame_idx={frame_idx})")
            return None
        frame_path, label_path = save_frames_and_labels(frame, labels, OUTPUT_DIR['person'], f"cam_{idx}")
        if frame_path is None or label_path is None:
            print(f"[Person Cam {idx}] Failed to save frame/labels (frame_idx={frame_idx})")
            return None
        img_crops = crop_object_cls_xyxy_nms(frame, labels, class_id=[1], nms_status=True, iou_threshold=0.6)
        if not img_crops:
            print(f"[Person Cam {idx}] No crops (frame_idx={frame_idx})")
            return None
        for crop in img_crops:
            predict_person_attributes(crop, PERSON_ATTRIBUTES_CONFIG, OUTPUT_DIR['person_attributes'], f"cam_{idx}")
            return True
        return None
    except Exception as e:
        print(f"[Person Cam {idx}] Error: {e}")
        return None
    
def process_helmet_attributes_cam(url, idx, frame_idx):
    try:
        cam_id = f"cam_{idx}"
        # # Step 1: Capture frame from RTSP
        frame = capture_from_rtsp(url)
        if frame is None:
            print(f"[Helmet Cam {idx}] Failed to capture frame (frame_idx={frame_idx})")
            return None
        
        # # Step 2: Detect vehicles (motorcycles) using YOLO
        labels = detecting_objects_by_yolo(frame, VEHICLE_CONFIG)
        if labels is None:
            print(f"[Helmet Cam {idx}] Detection failed (frame_idx={frame_idx})")
            return None
        
        # # Step 3: Save frame and labels for reference
        # frame_path, label_path = save_frames_and_labels(frame, labels, OUTPUT_DIR['helmet'], f"cam_{idx}")
        # if frame_path is None or label_path is None:
        #     print(f"[Helmet Cam {idx}] Failed to save frame/labels (frame_idx={frame_idx})")
        #     return None

        # # Step 4: Crop detected vehicles
        img_crops = crop_object_cls_xyxy_nms(
                        frame, labels, 
                        class_id=5,  # Vehicle class ID
                        nms_status=True, 
                        # iou_threshold=iou_threshold,
                        min_crop_size =200,
                        expand_bbox_tblr=(25,5,15,15) # (p_t, p_b, p_l, p_r)
                    )
        if not img_crops:
            print(f"[Helmet Cam {idx}] No crops (frame_idx={frame_idx})")
            return None
        
        # # Step 5: Run attribute prediction on each crop
        has_result = False
        for crop_idx, crop in enumerate(img_crops, start=1):
            labels_crop = detecting_objects_by_yolo(crop, HELMET_VEHICLE_CONFIG)
            if not labels_crop:
                print(f"[Helmet Cam {idx}] No vehicles detected in crop {crop_idx} (frame_idx={frame_idx})")
                continue
            result = save_frames_and_labels(
                crop, labels_crop, OUTPUT_DIR['helmet'], cam_id, class_condition=[(0,), (1, 2), (1, 3), (1, 4), (2, 3),(2, 4), (3, 4)]) # (hat) or (helmet & hood) or (helmet & no_helmet) or ....
                # crop, labels_crop, OUTPUT_DIR['helmet'], cam_id, class_condition=[0, 2, 4]) ## 0: hat, 1: helmet, 2: hood, 3: no_helmet, 4: smart_phone 
                # crop, labels_crop, OUTPUT_DIR['helmet'], cam_id)
            if result is not None:
                has_result = True
                print(f"[Helmet Cam {idx}] Crop {crop_idx} processed successfully")
        return has_result if has_result else None
    except Exception as e:
        print(f"[Helmet Cam {idx}] Error: {e}")
        return None