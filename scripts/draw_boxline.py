import pandas as pd
import matplotlib.pyplot as plt


# 读取csv文件
# data_1 = pd.read_csv("/users/X1aoyang/LegoAlloc/build/microbench/detail_result_1_cxl_frag_1718361757_.csv").T.values.tolist()
# data_2 = pd.read_csv("/users/X1aoyang/LegoAlloc/build/microbench/detail_result_2_cxl_frag_1718360559_.csv").T.values.tolist()
# data_4 = pd.read_csv("/users/X1aoyang/LegoAlloc/build/microbench/detail_result_4_cxl_frag_1718359997_.csv").T.values.tolist()
# data_8 = pd.read_csv("/users/X1aoyang/LegoAlloc/build/microbench/detail_result_8_cxl_frag_1718359684_.csv").T.values.tolist()

data = pd.read_csv("/users/X1aoyang/LegoAlloc/build/microbench/detail_result_20_share_frag_1719992441_.csv").T.values.tolist()
# print(data)

labels = ['1']
# labels = ['1', '2', '4', '8']

fig, ax = plt.subplots()

# 绘制矩形箱线图
ax.boxplot(data, showfliers=True, tick_labels=labels,
    flierprops={        # 设置异常点属性
        'marker': '.', 'markersize': 1, 'markeredgewidth': 0.75, 'markerfacecolor': '#ee5500', 'markeredgecolor': '#ee5500'
    },)
ax.set_xlabel("Thread number (10% memory used)")
ax.set_ylabel("Latency(us)")
plt.yscale('log')

plt.savefig('./test.png')