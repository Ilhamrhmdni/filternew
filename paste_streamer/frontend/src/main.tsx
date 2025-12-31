import React, { useEffect, useMemo, useRef, useState } from "react";
import ReactDOM from "react-dom/client";
import { Streamlit, withStreamlitConnection } from "streamlit-component-lib";

type Args = {
  chunk_chars?: number;
  ack_seq?: number;
};

type Payload =
  | { type: "start"; job_id: string; total_chars: number }
  | { type: "chunk"; job_id: string; seq: number; data: string }
  | { type: "done"; job_id: string };

function PasteStreamer(props: any) {
  const args = (props.args || {}) as Args;

  const chunkChars = Math.max(10000, Number(args.chunk_chars ?? 2_000_000));
  const ackSeq = Number(args.ack_seq ?? -1);

  const jobIdRef = useRef<string>("");
  const textRef = useRef<string>("");
  const nextSeqRef = useRef<number>(0);

  const [status, setStatus] = useState<"idle" | "ready" | "sending" | "done">("idle");
  const [totalChars, setTotalChars] = useState<number>(0);
  const [sentSeq, setSentSeq] = useState<number>(-1);

  const canSend = useMemo(() => status === "sending" && jobIdRef.current && textRef.current, [status]);

  useEffect(() => {
    Streamlit.setFrameHeight(230);
  }, []);

  // Handshake: hanya kirim chunk berikutnya saat Python sudah ACK chunk sebelumnya
  useEffect(() => {
    if (!canSend) return;

    const expectedAck = nextSeqRef.current - 1;
    if (ackSeq !== expectedAck) return;

    const start = nextSeqRef.current * chunkChars;
    const end = Math.min(textRef.current.length, start + chunkChars);

    if (start >= textRef.current.length) {
      const donePayload: Payload = { type: "done", job_id: jobIdRef.current };
      Streamlit.setComponentValue(donePayload);
      setStatus("done");
      setSentSeq(nextSeqRef.current - 1);
      return;
    }

    const chunk = textRef.current.slice(start, end);
    const payload: Payload = {
      type: "chunk",
      job_id: jobIdRef.current,
      seq: nextSeqRef.current,
      data: chunk
    };

    Streamlit.setComponentValue(payload);
    setSentSeq(nextSeqRef.current);
    nextSeqRef.current += 1;
  }, [ackSeq, canSend, chunkChars]);

  const handlePaste = async (e: React.ClipboardEvent<HTMLTextAreaElement>) => {
    // IMPORTANT: cegah textarea menampung teks panjang (DOM freeze)
    e.preventDefault();

    const pasted = e.clipboardData.getData("text") || "";
    if (!pasted) return;

    // reset job
    const jobId = (crypto as any)?.randomUUID ? crypto.randomUUID() : String(Date.now());
    jobIdRef.current = jobId;
    textRef.current = pasted;
    nextSeqRef.current = 0;
    setTotalChars(pasted.length);

    // kirim event start (Python akan truncate file + set ack=-1)
    const startPayload: Payload = { type: "start", job_id: jobId, total_chars: pasted.length };
    Streamlit.setComponentValue(startPayload);

    setStatus("sending");
    setSentSeq(-1);
  };

  const clear = () => {
    jobIdRef.current = "";
    textRef.current = "";
    nextSeqRef.current = 0;
    setTotalChars(0);
    setSentSeq(-1);
    setStatus("idle");
    Streamlit.setComponentValue(null);
  };

  return (
    <div style={{ fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, Arial", padding: 8 }}>
      <textarea
        onPaste={handlePaste}
        placeholder="Paste data di sini (tanpa header)."
        style={{
          width: "100%",
          height: 120,
          resize: "none",
          borderRadius: 8,
          border: "1px solid rgba(255,255,255,0.15)",
          background: "rgba(255,255,255,0.04)",
          color: "white",
          padding: 10,
          boxSizing: "border-box",
          outline: "none"
        }}
      />
      <div style={{ marginTop: 8, display: "flex", gap: 8, alignItems: "center" }}>
        <button
          onClick={clear}
          style={{
            borderRadius: 8,
            border: "1px solid rgba(255,255,255,0.2)",
            background: "rgba(255,255,255,0.06)",
            color: "white",
            padding: "6px 10px",
            cursor: "pointer"
          }}
        >
          Clear
        </button>

        <div style={{ opacity: 0.85, fontSize: 12 }}>
          {status === "idle" && "Siap menerima paste"}
          {status === "sending" && `Mengirim... (${Math.max(sentSeq, 0)} chunk)`}
          {status === "done" && `Selesai (total ${totalChars.toLocaleString()} chars)`}
        </div>
      </div>
    </div>
  );
}

const Connected = withStreamlitConnection(PasteStreamer);

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <Connected />
  </React.StrictMode>
);
