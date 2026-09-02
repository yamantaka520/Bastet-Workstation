export const locales = ["zh-Hant", "zh-Hans", "en", "ja", "ko"] as const;
export type Locale = (typeof locales)[number];

const messages = {
  "zh-Hant": { title: "Bastet Workstation", milestone: "M1 桌面與背景服務基礎", status: "正在建立可復原的本機工作空間", connecting: "正在連接本機服務", ready: "本機服務已連線", offline: "本機服務無法連線", retry: "重新連線", autostart: "登入時自動啟動（選用）" },
  "zh-Hans": { title: "Bastet Workstation", milestone: "M1 桌面与后台服务基础", status: "正在建立可恢复的本地工作空间", connecting: "正在连接本地服务", ready: "本地服务已连接", offline: "无法连接本地服务", retry: "重新连接", autostart: "登录时自动启动（可选）" },
  en: { title: "Bastet Workstation", milestone: "M1 desktop and daemon foundation", status: "Building a recoverable local workspace", connecting: "Connecting to local daemon", ready: "Local daemon connected", offline: "Local daemon unavailable", retry: "Reconnect", autostart: "Start automatically at login (opt-in)" },
  ja: { title: "Bastet Workstation", milestone: "M1 デスクトップとデーモン基盤", status: "復元可能なローカルワークスペースを構築中", connecting: "ローカルデーモンに接続中", ready: "ローカルデーモンに接続しました", offline: "ローカルデーモンに接続できません", retry: "再接続", autostart: "ログイン時に自動起動（任意）" },
  ko: { title: "Bastet Workstation", milestone: "M1 데스크톱 및 데몬 기반", status: "복구 가능한 로컬 작업 공간을 구축하는 중", connecting: "로컬 데몬에 연결 중", ready: "로컬 데몬 연결됨", offline: "로컬 데몬에 연결할 수 없음", retry: "다시 연결", autostart: "로그인 시 자동 시작(선택 사항)" },
} as const satisfies Record<Locale, Record<string, string>>;

export type MessageKey = keyof (typeof messages)["en"];
export function translate(locale: Locale, key: MessageKey): string {
  return messages[locale]?.[key] ?? messages.en[key] ?? `[missing:${key}]`;
}
