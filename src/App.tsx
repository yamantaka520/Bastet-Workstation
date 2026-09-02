import { invoke } from "@tauri-apps/api/core";
import { disable, enable, isEnabled } from "@tauri-apps/plugin-autostart";
import { useCallback, useEffect, useState } from "react";
import { locales, type Locale, translate } from "./i18n";
import "./styles.css";

type ConnectionState = "connecting" | "ready" | "offline";
type DaemonSnapshot = {
  protocol_version: number;
  daemon_id: string;
  revision: number;
  lifecycle: string;
};

export function App() {
  const [locale, setLocale] = useState<Locale>("zh-Hant");
  const [connection, setConnection] = useState<ConnectionState>("connecting");
  const [snapshot, setSnapshot] = useState<DaemonSnapshot | null>(null);
  const [autostart, setAutostart] = useState(false);

  const reconnect = useCallback(async () => {
    setConnection("connecting");
    try {
      const next = await invoke<DaemonSnapshot>("daemon_snapshot");
      if (next.protocol_version !== 1) throw new Error("protocol mismatch");
      setSnapshot(next);
      setConnection("ready");
    } catch {
      setSnapshot(null);
      setConnection("offline");
    }
  }, []);

  useEffect(() => {
    void reconnect();
    const interval = window.setInterval(() => void reconnect(), 5_000);
    return () => window.clearInterval(interval);
  }, [reconnect]);

  useEffect(() => {
    void isEnabled().then(setAutostart).catch(() => setAutostart(false));
  }, []);

  const changeAutostart = async (enabled: boolean) => {
    if (enabled) await enable();
    else await disable();
    setAutostart(await isEnabled());
  };

  return (
    <main>
      <header>
        <p className="eyebrow">{translate(locale, "milestone")}</p>
        <h1>{translate(locale, "title")}</h1>
        <p>{translate(locale, "status")}</p>
      </header>
      <label>
        <span className="sr-only">Language</span>
        <select aria-label="Language" value={locale}
          onChange={(event) => setLocale(event.target.value as Locale)}>
          {locales.map((candidate) => <option key={candidate} value={candidate}>{candidate}</option>)}
        </select>
      </label>
      <label className="preference">
        <input
          type="checkbox"
          checked={autostart}
          onChange={(event) => void changeAutostart(event.target.checked)}
        />
        {translate(locale, "autostart")}
      </label>
      <section aria-labelledby="authority-heading">
        <h2 id="authority-heading">Local daemon authority</h2>
        <p role="status" data-state={connection}>{translate(locale, connection)}</p>
        {snapshot ? (
          <dl>
            <dt>Protocol</dt><dd>{snapshot.protocol_version}</dd>
            <dt>Revision</dt><dd>{snapshot.revision}</dd>
            <dt>Lifecycle</dt><dd>{snapshot.lifecycle}</dd>
          </dl>
        ) : (
          <button type="button" onClick={() => void reconnect()}>
            {translate(locale, "retry")}
          </button>
        )}
      </section>
    </main>
  );
}
