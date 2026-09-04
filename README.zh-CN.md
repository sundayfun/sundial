# Sundial

[English](README.md)

Sundial 是一个轻量、可扩展、类型安全的 Go 配置 SDK，提供内存读取、持久化写入
和实时更新能力。

## 为什么选择 Sundial

- **类型安全访问**：应用直接读取自己定义的配置结构体，不再使用字符串路径和 `any`。
- **快速读取**：`Get` 只读取内存快照。
- **持久化写入**：`Put` 有条件地保存完整的强类型配置文档。
- **实时更新**：自动重新加载将外部变化同步到内存。
- **存储和格式可扩展**：配置源实现 `Provider`；默认使用 JSON，其他格式通过 Codec 扩展。

每个 `Client` 管理一份完整配置文档。

## 安装

```sh
go get github.com/sundayfun/sundial
```

## 快速开始

定义应用拥有的配置结构：

```go
type Config struct {
	Server struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	} `json:"server"`
	Debug bool `json:"debug"`
}
```

以下以 S3 为例：

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

configStore, err := s3provider.New[Config](ctx, &s3provider.Config{
	Region: "us-east-1",
	Bucket: "my-config-bucket",
	Key:    "production/app.json",
})
if err != nil {
	log.Fatal(err)
}
```

### 读取

`Get` 从当前内存快照返回独立的 `Entry`：

```go
entry, err := configStore.Get()
if err != nil {
	log.Fatal(err)
}

fmt.Println(entry.Value.Server.Port)
```

### 写入

修改配置值，然后将同一个 `Entry` 传回进行条件写入：

```go
entry.Value.Server.Port = 9090
entry, err = configStore.Put(ctx, entry)
if err != nil {
	if sundial.IsConflict(err) {
		// 重新加载、应用本次修改，然后按需重试。
		log.Print("保存前配置已发生变化")
		return
	}
	log.Fatal(err)
}
```

`Put` 使用 `entry.Metadata` 中的 Revision，并返回经 Codec 解码且带有新
Revision 的已保存 `Entry`；Revision 过期时返回 `ErrConflict`。它不会自动
合并或重试。

默认使用 JSON；其他格式可通过 `WithCodec` 配置。具体存储实现位于
`provider/<source>`。

完整示例见 [S3 示例](examples/s3)。

## 参考资料

- [koanf](https://github.com/knadh/koanf)
- [Viper](https://github.com/spf13/viper)

## 行为约定

- 配置文档不存在时，`New` 或 `Reload` 返回 `ErrNotFound`。
- 配置文档为空或仅包含空白字符时，`New`、`Put` 或 `Reload` 返回解码错误。
- `Put` 失败或发生冲突时，当前内存快照保持不变。
- 重新加载失败时，保留上一份有效配置。
- `WithOnChange` 接收新发布的 `Entry`；`WithOnError` 接收自动重新加载错误。
- 取消传给 `New` 的 context 会停止自动重新加载。
- `Get` 支持并发调用。同一实例的 `Put` 会串行执行，陈旧 Revision 会返回
  `ErrConflict`。

## 许可证

[MIT](LICENSE)
