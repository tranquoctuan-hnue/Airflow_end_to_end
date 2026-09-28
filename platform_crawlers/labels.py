"""
27 nhãn sự việc cần thu thập — đúng danh sách classes_full.txt (gốc UCF-Crime).

Mỗi nhãn có:
    dir_name  Tên thư mục lưu video → <OUTPUT_BASE>/<dir_name>/
              OUTPUT_BASE mặc định là NAS pvn_share (crawler_core/downloader.py),
              đổi bằng env CRAWL_VIDEO_OUTPUT_DIR.
              Dùng CamelCase không dấu cách để an toàn trên filesystem
              và khớp quy ước của UCF-Crime (RoadAccident, Shoplifting...).
    display   Tên hiển thị trong log
    desc      Mô tả đưa vào prompt Gemini — càng cụ thể thì keyword sinh ra
              càng đúng loại sự việc, tránh lẫn giữa các nhãn gần nhau
              (Robbery vs Shoplifting, Fighting vs Abuse).
    en        Keyword tiếng Anh tĩnh, dùng khi Gemini lỗi/hết quota.

Chọn nhãn cần crawl:
    export CRAWL_LABELS="Arson|Robbery|Shooting"     # chỉ 3 nhãn này
    (để trống = chạy tất cả nhãn)

    display  = đúng tên trong danh sách lớp gán nhãn (classes_full.txt), nên
               CRAWL_LABELS nhận cả "Road Accident" lẫn "RoadAccident".

Các nhãn gần nhau PHẢI có mô tả loại trừ nhau (Gemini sinh từ khóa theo desc):
    Robbery  ⊃ tách riêng: Gun-toting robber, Knife-wielding robber, Snatching
    Fighting ⊃ tách riêng: Armed fight
    Shooting / Shoot down / Armed suspect
    Stealing (gồm cả trộm trong cửa hàng — Shoplifting cũ đã gộp vào đây)
             / Burglary / Car theft / Motor theft
    Collapse (gồm cả trượt/vấp ngã — FallOver cũ đã gộp vào đây) / Unconscious
"""

import os

LABELS: dict[str, dict] = {

    'Abuse': {
        'display': 'Abuse',
        'desc': (
            'one person physically abusing or beating another person — '
            'domestic violence, child abuse, elder abuse, a caregiver or '
            'staff member hitting someone. NOT a mutual fight between equals.'
        ),
        'en': ['cctv abuse caught camera', 'security camera caught abuse',
               'surveillance footage domestic violence'],
    },

    'Arrest': {
        'display': 'Arrest',
        'desc': (
            'police officers detaining, handcuffing or arresting a suspect, '
            'recorded by a fixed surveillance camera.'
        ),
        'en': ['cctv police arrest footage', 'security camera arrest suspect',
               'surveillance camera police detain'],
    },

    'Arson': {
        'display': 'Arson',
        'desc': (
            'a person deliberately setting fire to a building, vehicle or '
            'property — pouring fuel, igniting, then fleeing. '
            'NOT an accidental fire, NOT a firefighter response video.'
        ),
        'en': ['cctv arson caught fire', 'security camera setting fire',
               'surveillance footage arson suspect'],
    },

    'Explosion': {
        'display': 'Explosion',
        'desc': (
            'a blast or explosion captured by a fixed camera — gas explosion, '
            'transformer blast, vehicle explosion, industrial accident.'
        ),
        'en': ['cctv explosion moment caught', 'security camera explosion blast',
               'surveillance camera gas explosion'],
    },

    'Fighting': {
        'display': 'Fighting',
        'desc': (
            'two or more people brawling or fighting each other with bare '
            'hands — a street fight, bar brawl, group scuffle. Mutual violence '
            'between people, NOT one-sided abuse. Fights with knives, machetes, '
            'sticks or other weapons are ArmedFight.'
        ),
        'en': ['cctv street fight caught', 'security camera brawl footage',
               'surveillance camera fight store'],
    },

    'RoadAccident': {
        'display': 'RoadAccident',
        'desc': (
            'a traffic collision recorded by a fixed roadside or intersection '
            'camera — car crash, motorbike accident, pedestrian struck by a '
            'vehicle. NOT dashcam footage from inside a moving car.'
        ),
        'en': ['cctv traffic accident intersection', 'security camera car crash',
               'surveillance camera road accident'],
    },

    'Vandalism': {
        'display': 'Vandalism',
        'desc': (
            'a person deliberately destroying or defacing property — smashing '
            'windows, keying cars, spraying graffiti, kicking down fixtures. '
            'Destruction WITHOUT stealing anything.'
        ),
        'en': ['cctv vandalism caught camera', 'security camera smashing windows',
               'surveillance footage property damage'],
    },

    'Robbery': {
        'display': 'Robbery',
        'desc': (
            'a robbery using force or threat where NO gun or knife is clearly '
            'visible — robbers overpowering, pushing or intimidating staff or a '
            'victim to take money or goods. Victim is present and confronted. '
            'Robbery with a visible gun is GunRobber, with a visible knife is '
            'KnifeRobber, grabbing and running off with a bag or phone is '
            'Snatching.'
        ),
        'en': ['cctv armed robbery store', 'security camera robbery caught',
               'surveillance footage store holdup'],
    },

    'Shooting': {
        'display': 'Shooting',
        'desc': (
            'a firearm being discharged at people, recorded by a fixed '
            'surveillance camera — a shooting incident or shootout in a shop, '
            'street or building, focus on the gunfire itself. A person being '
            'hit and falling is ShootDown.'
        ),
        'en': ['cctv shooting incident caught', 'security camera gunman footage',
               'surveillance camera shooting store'],
    },

    # ── Nhãn bổ sung theo classes_full.txt ──────────────────────────────────

    'ArmedSuspect': {
        'display': 'Armed suspect',
        'desc': (
            'a person carrying or brandishing a weapon (gun, knife, machete, '
            'bat) — walking around, lurking, threatening or waving it — BEFORE '
            'or WITHOUT an actual robbery, fight or shooting taking place.'
        ),
        'en': ['cctv man with gun walking', 'security camera suspect with machete',
               'surveillance footage armed man lurking'],
    },

    'Burglary': {
        'display': 'Burglary',
        'desc': (
            'breaking into a house, shop, office or warehouse to steal — '
            'forcing a lock or door, climbing a wall or fence, entering through '
            'a window, usually at night when nobody is present. No victim is '
            'confronted (that would be Robbery).'
        ),
        'en': ['cctv burglar breaking into house', 'security camera break in night',
               'surveillance footage burglary shop'],
    },

    'Collapse': {
        'display': 'Collapse',
        'desc': (
            'EITHER a structure collapsing — a building, wall, ceiling, bridge, '
            'scaffolding or tree falling down — OR a person falling down on '
            'their own: slipping on a wet floor, tripping, falling on stairs, '
            'or suddenly collapsing from a medical cause (stroke, heart attack, '
            'fainting). No other person attacking them.'
        ),
        'en': ['cctv building collapse caught', 'security camera person slip and fall',
               'cctv man suddenly collapses'],
    },

    'Drowning': {
        'display': 'To drown',
        'desc': (
            'a person drowning or struggling in water — a swimming pool, river, '
            'lake, canal or sea — often a child slipping under in a pool, '
            'recorded by a fixed camera.'
        ),
        'en': ['cctv child drowning pool', 'security camera drowning swimming pool',
               'surveillance footage person drowning'],
    },

    'ArmedFight': {
        'display': 'Armed fight',
        'desc': (
            'people fighting each other WITH weapons — knives, machetes, swords, '
            'sticks, bats or bottles — a gang fight or street clash with '
            'weapons. Bare-hand fights are Fighting.'
        ),
        'en': ['cctv gang fight machete', 'security camera knife fight street',
               'surveillance footage fight with weapons'],
    },

    'CarTheft': {
        'display': 'Car theft',
        'desc': (
            'stealing a car or breaking into a parked car — smashing a window, '
            'unlocking it and driving the car away, or taking items from inside '
            'it. About cars, NOT motorbikes.'
        ),
        'en': ['cctv car theft caught', 'security camera thief stealing car',
               'surveillance footage car break in'],
    },

    'MotorTheft': {
        'display': 'Motor theft',
        'desc': (
            'stealing a motorbike or scooter — breaking the ignition lock, '
            'pushing or riding away a parked motorbike in front of a house, '
            'shop or parking lot.'
        ),
        'en': ['cctv motorbike theft caught', 'security camera stealing motorcycle',
               'surveillance footage scooter thief'],
    },

    'Normal': {
        'display': 'Normal',
        'desc': (
            'ordinary everyday CCTV footage where NOTHING abnormal happens — '
            'normal traffic, people walking, customers shopping, a quiet office '
            'or parking lot. The negative class of the dataset.'
        ),
        'en': ['cctv footage normal day street', 'security camera footage shop customers',
               'surveillance camera parking lot footage'],
    },

    'PourPetrol': {
        'display': 'Pour petrol',
        'desc': (
            'a person pouring or splashing petrol/gasoline onto a house, shop, '
            'vehicle or another person, usually before setting it on fire. '
            'NOT refuelling at a petrol station. If the clip is mainly the '
            'fire itself, that is Arson.'
        ),
        'en': ['cctv man pouring petrol house', 'security camera pouring gasoline shop',
               'surveillance footage splashing petrol'],
    },

    'Riot': {
        'display': 'Riot',
        'desc': (
            'a violent crowd — a mob smashing, looting, throwing stones or '
            'objects, setting things on fire or clashing with police. Many '
            'people, not a fight between a few individuals.'
        ),
        'en': ['cctv riot mob looting', 'security camera rioters smashing',
               'surveillance footage crowd violence'],
    },

    'GunRobber': {
        'display': 'Gun-toting robber',
        'desc': (
            'a robbery where the robber clearly holds or points a GUN at staff '
            'or a victim — armed holdup of a shop, bank, jewellery store or '
            'petrol station.'
        ),
        'en': ['cctv armed robbery gun store', 'security camera robber pointing gun',
               'surveillance footage gunpoint robbery'],
    },

    'KnifeRobber': {
        'display': 'Knife-wielding robber',
        'desc': (
            'a robbery where the robber clearly holds or threatens with a KNIFE, '
            'machete or other blade — holding up a shop, taxi driver or passer-by '
            'at knifepoint.'
        ),
        'en': ['cctv knife robbery store', 'security camera robber with knife',
               'surveillance footage knifepoint robbery'],
    },

    'ShootDown': {
        'display': 'Shoot down',
        'desc': (
            'a person being shot and falling to the ground — a victim hit by '
            'gunfire, or police/security officers shooting down an attacker or '
            'suspect. Focus is the person hit and going down.'
        ),
        'en': ['cctv man shot falls down', 'security camera police shoot suspect',
               'surveillance footage victim shot'],
    },

    'Smoke': {
        'display': 'Smok',
        'desc': (
            'smoke appearing or spreading — smoke rising from a building, room, '
            'vehicle, electrical panel or machine, early smoke before flames '
            'are visible. NOT cigarette smoking, NOT an explosion.'
        ),
        'en': ['cctv smoke fire starting', 'security camera smoke coming out building',
               'surveillance footage electrical fire smoke'],
    },

    'Stampede': {
        'display': 'Stampede',
        'desc': (
            'a crowd crush or stampede — a panicked crowd running, pushing and '
            'trampling people, people falling over each other at an exit, '
            'stairway or event.'
        ),
        'en': ['cctv stampede crowd crush', 'security camera crowd panic running',
               'surveillance footage stampede stairs'],
    },

    'Stealing': {
        'display': 'Stealing',
        'desc': (
            'STEALTHY theft without force or confrontation — shoplifting '
            '(hiding goods inside a shop), pickpocketing, taking an unattended '
            'bag, phone or package. NOT breaking in (Burglary), NOT cars or '
            'motorbikes (CarTheft / MotorTheft), NOT grabbing from a victim '
            '(Snatching).'
        ),
        'en': ['cctv shoplifting caught camera', 'security camera pickpocket caught',
               'surveillance footage stealing package'],
    },

    'Snatching': {
        'display': 'Snatching',
        'desc': (
            'snatch theft — grabbing a bag, phone or necklace from a victim and '
            'running or riding off, often by thieves on a motorbike. Quick grab, '
            'no weapon shown.'
        ),
        'en': ['cctv bag snatching motorbike', 'security camera phone snatch',
               'surveillance footage snatch thief'],
    },

    'Unconscious': {
        'display': 'Unconscious',
        'desc': (
            'a person lying unconscious and motionless — fainted, overdosed, '
            'drunk or injured — on a floor, street or in a vehicle, often being '
            'discovered or helped by others. The state of lying still, not the '
            'moment of falling.'
        ),
        'en': ['cctv man lying unconscious', 'security camera person passed out',
               'surveillance footage found unconscious'],
    },
}

ALL_LABELS = tuple(LABELS)


def enabled_labels() -> list[str]:
    """Nhãn cần crawl, lọc theo env CRAWL_LABELS. Sai tên → cảnh báo, bỏ qua."""
    raw = os.environ.get('CRAWL_LABELS', '').strip()
    if not raw:
        return list(ALL_LABELS)

    wanted, unknown = [], []
    for item in raw.split('|'):
        item = item.strip()
        if not item:
            continue
        # Cho phép nhập không phân biệt hoa/thường và cả tên hiển thị có dấu cách
        match = next(
            (k for k in ALL_LABELS
             if k.lower() == item.lower()
             or LABELS[k]['display'].lower() == item.lower()
             or k.lower() == item.lower().replace(' ', '')),
            None,
        )
        if match:
            wanted.append(match)
        else:
            unknown.append(item)

    if unknown:
        import logging
        logging.getLogger(__name__).warning(
            f"[Labels] Không nhận diện được: {unknown}. Hợp lệ: {', '.join(ALL_LABELS)}"
        )
    return wanted or list(ALL_LABELS)


def static_keywords(label: str) -> list[str]:
    return LABELS.get(label, {}).get('en', [])


def describe(label: str) -> str:
    return LABELS.get(label, {}).get('desc', label)


def display(label: str) -> str:
    return LABELS.get(label, {}).get('display', label)
