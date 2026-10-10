# Sundial

[![Go Reference](https://pkg.go.dev/badge/github.com/sundayfun/sundial.svg)](https://pkg.go.dev/github.com/sundayfun/sundial)

[English](README.md)

Sundial 是一个轻量、可扩展、类型安全的 Go 配置 SDK，提供内存读取、持久化写入
和实时更新能力。

## 为什么选择 Sundial

- **类型安全访问**：应用直接读取自己定义的配置结构体，不再使用字符串路径和 `any`。
- **快速读取**：`Get` 返回内存中已解析的快照，无需再次解码或深拷贝。
- **持久化写入**：`Update` 有条件地保存完整的强类型配置文档。
- **版本历史**：支持查看历史版本和恢复配置。
- **实时更新**：自动重新加载将外部变化同步到内存。
- **存储和格式可扩展**：配置源实现 `Provider`；默认使用 JSON，其他格式通过 Codec 扩展。

每个 `Client` 管理一份完整配置文档。

## 使用效果

![Sundial 内存读取、并发写入保护与历史恢复示意图](docs/images/sundial-overview.png)

例如，两个 Client 都缓存了版本 A：第一个更新后生成 B，第二个仍基于 A 写入时会收到
`ErrConflict`。之后从 C 恢复 A 的配置，会生成内容与 A 相同的新版本 D，保留完整历史。

## 安装

```sh
go get github.com/sundayfun/sundial
```

## 快速开始

以下示例读取并更新 S3 中已有的 JSON 配置。
凭据配置和首次发布见 [S3 示例](examples/s3)。

```go
package main

import (
    "context"
    "fmt"
    "log"

    s3provider "github.com/sundayfun/sundial/provider/s3"
)

type Config struct {
    Port int `json:"port"`
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    store, err := s3provider.New[Config](ctx, &s3provider.Config{
        Region: "us-east-1",
        StorageConfig: s3provider.StorageConfig{
            Bucket:             "my-config-bucket",
            CurrentRevisionKey: "production/app/metadata.yaml",
            RevisionKeyPrefix:  "production/app/",
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    entry := store.Get()
    fmt.Println(entry.Value.Port)

    if _, err := store.Update(ctx, func(config *Config) error {
        config.Port = 9090
        return nil
    }); err != nil {
        log.Fatal(err)
    }
}
```

`Get` 返回共享的只读配置快照及其版本，无 error 返回，也不进行编码、解码或
深拷贝。不要修改返回值，包括其中的 map、slice 和指针。Go 不强制只读，需要
调用方遵守这个约定。`Update`、`RestoreRevision` 成功后返回的结果，以及 `OnChange` 回调
收到的配置，同样是共享只读快照。

普通修改直接调用 `Update(ctx, func(*T) error)`，不需要先调用 `Get`。它是 Client
唯一的普通写入接口。内部先解码缓存的配置原文，生成独立草稿，再执行修改回调；
发布前编码并解码修改结果，以验证配置并隔离缓存快照与回调草稿，最后通过版本条件发布。
每次更新执行一次编码、两次解码；快照同时保留对应的配置原文。
编码、解码、回调或发布失败时都保留原快照；返回结果是共享只读配置。
调用期间不要并发修改回调草稿。回调内不要对同一 Client 调用 `Update`、`Reload`
或 `RestoreRevision`，因为这些操作会获取同一个写锁。

`New(ctx, provider, opts...)` 无需调用方提供 clone 函数。草稿通过 Codec 重新解析
缓存的原文；若 Codec 注入动态值，草稿可能与当前已解析的快照不同。
Codec 每次 `Decode` 必须创建独立的可变对象，不能保留或复用这些对象的引用；
`Encode` 不得修改输入。

`Update` 保存完整文档，以缓存快照的版本进行比较并交换（CAS）。
如果另一个 Client 先发布了新版本，过期写入会返回 `ErrConflict`，不会自动合并或重试。
取消 context 会停止自动加载。写入或加载失败时，保留内存中上一份有效配置。

## 文档

- [S3 示例](examples/s3)：环境配置、首次发布和条件写入。
- [API 文档](https://pkg.go.dev/github.com/sundayfun/sundial)：版本历史、Codec 和加载回调。

## 许可证

[MIT](LICENSE)
