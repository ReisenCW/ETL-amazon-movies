# 从原始数据集读取所有product_id（去重）
def get_all_product_ids():
    product_ids = set()
    with open("../data/movies.txt", "r", encoding="latin-1") as f:
        for line in f:
            line = line.strip()
            if line.startswith("product/productId:"):
                pid = line.split(": ")[1]
                product_ids.add(pid)
    return list(product_ids)

if __name__ == "__main__":
    # 将list写入文件product_ids.txt，每行一个
    product_ids = get_all_product_ids()
    with open("../data/products_id.txt", "w", encoding="utf-8") as f:
        for pid in product_ids:
            f.write(f"{pid}\n")