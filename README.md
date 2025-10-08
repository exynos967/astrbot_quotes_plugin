# quotes

一个 AstrBot 插件：
- 上传语录：在“回复某人的消息”时发送 `上传`（可附带图片）。图片将按群分目录保存到 `data/quotes/images/<群号>/`；私聊为 `data/quotes/images/private_<发送者QQ>/`。
- 随机语录：发送 `语录`；
  - 可选：`语录 @某人` 仅随机该用户的语录（按 QQ 号匹配）。
  - 若该语录含用户上传图片，将直接发送原图；否则渲染文本语录图。
- 删除语录：在“回复机器人刚发送的随机语录消息”时发送 `删除`（或 `删除语录`），即可删除该语录。

数据持久化于 AstrBot 根目录：
- 文本：`data/quotes/quotes.json`
- 图片：`data/quotes/images/*`（按群分目录）

Napcat/OneBot 平台默认使用 qlogo 头像；也可在配置中改为 platform。

依赖：
- `httpx`（下载远程图片）
