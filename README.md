# rate-limiter
- 一个基于redis+lua的限速器

# 支持功能
| 功能  | 含义  |   算法   |
| :---: | :---: | :------: |
| quota | 配额  |  计数器  |
| rate  | 限速  | 滑动窗口 |