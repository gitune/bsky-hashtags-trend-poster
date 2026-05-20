# Step 1: ビルドステージ
# Alpine Linuxベースの軽量なGoイメージを使用
FROM golang:1.26-alpine AS builder

# 作業ディレクトリの設定
WORKDIR /app

# go.modとsource fileをコピー
COPY go.mod .
COPY *.go .

# 依存関係を解決・ダウンロード
RUN go mod tidy && go mod download

# プログラムのビルド
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o hashtags-trend-poster .

# CA証明書をインストールし、最終イメージにコピーするために一時的な場所に配置
RUN apk --no-cache add ca-certificates && \
    cp /etc/ssl/certs/ca-certificates.crt /usr/local/share/ca-certificates/ca-certificates.crt

# ユーザ＆グループを作成
RUN addgroup -S -g 1000 poster && adduser -S -u 1000 -G poster poster

# Step 2: 最終実行ステージ
# scratchはOSを含まない最小のイメージです
FROM scratch

# ステージ1で作成したユーザとグループ情報をコピーする
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

# ビルドしたバイナリを最終イメージにコピー
COPY --from=builder /app/hashtags-trend-poster /app/hashtags-trend-poster

# ビルドステージからCA証明書を最終イメージにコピー
COPY --from=builder /usr/local/share/ca-certificates/ca-certificates.crt /etc/ssl/certs/

# 作業ディレクトリの設定
WORKDIR /app

# 実行ユーザの設定
USER poster

# コンテナ起動時に実行するコマンド
CMD ["/app/hashtags-trend-poster"]
