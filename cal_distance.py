import math

def calculate_distance(v, theta, t=1):
    # 1. 将速度v转换为米每秒
    v_mps = v / 3.6

    # 2. 将时间t转换为秒
    t_s = t * 3600

    # 3. 计算物体A和B在球面上的大圆弧距离（单位：千米）
    great_circle_distance_A = (v_mps * t_s) / 1000  # 将距离转换为千米
    great_circle_distance_B = great_circle_distance_A

    # 4. 将夹角θ从角度制转换为弧度制
    theta_rad = math.radians(theta)

    # 5. 使用球面三角形的余弦定理计算物体A和B之间的距离S
    cos_S = math.cos(great_circle_distance_A / 6371) * math.cos(great_circle_distance_B / 6371) + math.sin(great_circle_distance_A / 6371) * math.sin(great_circle_distance_B / 6371) * math.cos(theta_rad)
    S_rad = math.acos(cos_S)

    # 6. 计算距离
    S = S_rad * 6371

    return S

# 示例
v = 36  # 速度：36 km/h
theta = 60  # 夹角：60°
S = calculate_distance(v, theta)
print(f"物体A和B之间的距离为：{S}千米")
