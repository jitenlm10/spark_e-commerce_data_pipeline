import { useState } from "react";
import { motion } from "framer-motion";
import GuideModel from "./GuideModel";

function speakWelcome() {
  const text =
    "Welcome to the Distributed E-commerce Analytics Portal. Ask me about views, carts, purchases, conversion rates, or anomalies.";
  const utterance = new SpeechSynthesisUtterance(text);
  utterance.rate = 1;
  utterance.pitch = 1;
  utterance.volume = 1;
  speechSynthesis.cancel();
  speechSynthesis.speak(utterance);
}

export default function HeroGuide({
  onWelcome,
  onAsk,
  aiResponse,
  loadingAI,
}) {
  const [question, setQuestion] = useState("");

  const handleSubmit = () => {
  alert(`Ask clicked: ${question}`);
  console.log("Ask clicked, question =", question);

  if (!question.trim()) return;

  if (!onAsk) {
    alert("onAsk is missing");
    console.error("onAsk is missing");
    return;
  }

  onAsk(question);
  setQuestion("");
};

  return (
    <section
      style={{
        display: "grid",
        gridTemplateColumns: "1.2fr 1fr",
        gap: "2rem",
        alignItems: "center",
        padding: "3rem 2rem",
        borderBottom: "1px solid rgba(148, 163, 184, 0.12)",
      }}
    >
      <div>
        <motion.h1
          initial={{ opacity: 0, y: -25 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          style={{
            fontSize: "3.2rem",
            marginBottom: "0.8rem",
            color: "#f8fafc",
            lineHeight: 1.1,
          }}
        >
          Distributed E-commerce Analytics Portal
        </motion.h1>

        <motion.p
          initial={{ opacity: 0, y: -15 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.7, delay: 0.1 }}
          style={{
            color: "#cbd5e1",
            fontSize: "1.1rem",
            lineHeight: 1.7,
            maxWidth: "800px",
            marginBottom: "1.5rem",
          }}
        >
          A premium analytics interface powered by PySpark distributed processing,
          FastAPI APIs, and interactive frontend visualization for funnel analysis,
          anomaly detection, and conversion insights.
        </motion.p>

        <div style={{ display: "flex", gap: "1rem", flexWrap: "wrap" }}>
          <button
            onClick={onWelcome || speakWelcome}
            style={{
              background: "#2563eb",
              color: "white",
              border: "none",
              padding: "0.9rem 1.3rem",
              borderRadius: "12px",
              cursor: "pointer",
              fontWeight: "bold",
            }}
          >
            Welcome Voice
          </button>
        </div>

        <div
          style={{
            marginTop: "1.2rem",
            maxWidth: "760px",
            display: "flex",
            gap: "0.8rem",
            alignItems: "center",
            padding: "0.5rem",
            borderRadius: "18px",
            background: "rgba(15, 23, 42, 0.72)",
            border: "1px solid rgba(148, 163, 184, 0.16)",
            boxShadow: "0 10px 30px rgba(0,0,0,0.22)",
          }}
        >
          <input
  type="text"
  value={question}
  onChange={(e) => {
    console.log("Typing:", e.target.value);
    setQuestion(e.target.value);
  }}
  onKeyDown={(e) => {
    if (e.key === "Enter") handleSubmit();
  }}
  placeholder="Ask about views, carts, purchases, conversion, or anomalies..."
  style={{
    flex: 1,
    background: "transparent",
    border: "none",
    outline: "none",
    color: "#e2e8f0",
    fontSize: "1rem",
    padding: "0.9rem 1rem",
            }}
          />

          <button
            onClick={handleSubmit}
            style={{
              background: "linear-gradient(135deg, #1d4ed8, #2563eb)",
              color: "white",
              border: "none",
              padding: "0.9rem 1.2rem",
              borderRadius: "12px",
              cursor: "pointer",
              fontWeight: "bold",
              minWidth: "90px",
            }}
          >
            Ask
          </button>
        </div>

        <div
          style={{
            marginTop: "1rem",
            maxWidth: "760px",
            minHeight: "120px",
            borderRadius: "16px",
            padding: "1rem 1.1rem",
            background: "rgba(15, 23, 42, 0.7)",
            border: "1px solid rgba(148, 163, 184, 0.16)",
            color: "#e2e8f0",
            lineHeight: 1.7,
            boxShadow: "0 10px 30px rgba(0,0,0,0.25)",
          }}
        >
          {loadingAI
            ? "Thinking..."
            : aiResponse ||
              "Ask a question about your analytics dashboard and I’ll explain it for you."}
        </div>
      </div>

      <motion.div
        initial={{ opacity: 0, x: 30 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.7, delay: 0.2 }}
        style={{
          minHeight: "420px",
          borderRadius: "24px",
          background:
            "linear-gradient(180deg, rgba(30,41,59,0.9), rgba(15,23,42,0.95))",
          border: "1px solid rgba(148, 163, 184, 0.18)",
          boxShadow: "0 10px 40px rgba(0,0,0,0.35)",
          position: "relative",
          overflow: "hidden",
        }}
      >
        <div
          style={{
            position: "absolute",
            inset: 0,
            background:
              "radial-gradient(circle at center, rgba(37,99,235,0.18), transparent 60%)",
            pointerEvents: "none",
          }}
        />

        <div
          style={{
            position: "relative",
            zIndex: 2,
            width: "100%",
            height: "420px",
          }}
        >
          <GuideModel />
        </div>
      </motion.div>
    </section>
  );
}