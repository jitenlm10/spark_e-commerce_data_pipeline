import { useEffect, useState } from "react";

import HeroGuide from "./HeroGuide";
import { motion } from "framer-motion";
import ArchitectureSection from "./ArchitectureSection";
import API, { askAssistant } from "./api";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  LineChart,
  Line,
  Legend,
} from "recharts";

function TypingInsight({ text, speed = 18 }) {
  const [displayed, setDisplayed] = useState("");

  useEffect(() => {
    let index = 0;
    setDisplayed("");

    const interval = setInterval(() => {
      index++;
      setDisplayed(text.slice(0, index));

      if (index >= text.length) {
        clearInterval(interval);
      }
    }, speed);

    return () => clearInterval(interval);
  }, [text, speed]);

  return <span>{displayed}</span>;
}


function AnimatedNumber({ end, duration = 2000, decimals = 0, suffix = "", separator = false }) {
  const [displayValue, setDisplayValue] = useState(0);

  useEffect(() => {
    let startTime = null;

    const animate = (currentTime) => {
      if (!startTime) startTime = currentTime;
      const progress = Math.min((currentTime - startTime) / duration, 1);
      const currentValue = end * progress;

      setDisplayValue(currentValue);

      if (progress < 1) {
        requestAnimationFrame(animate);
      }
    };

    requestAnimationFrame(animate);
  }, [end, duration]);

  const formattedValue = separator
    ? Math.round(displayValue).toLocaleString()
    : displayValue.toFixed(decimals);

  return <>{formattedValue}{suffix}</>;
}

function SummaryCard({ title, value,trend,delay, summary }) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 30 }}
      animate={{ opacity: 1, y: 0 }}
      whileHover={{ scale: 1.2,
        boxShadow: "0 12px 40px rgba(59,130,246,0.4)",
        border: "1px solid rgba(59,130,246,0.6)",
       }}
      transition={{ duration: 0.3, delay }}
      style={{
        background: "rgba(30, 41, 59, 0.95)",
        borderRadius: "18px",
        padding: "1.2rem",
        boxShadow: "0 8px 30px rgba(0,0,0,0.35)",
        border: "1px solid rgba(148, 163, 184, 0.15)",
        cursor: "pointer",
      }}
    >
      <div style={{ color: "#94a3b8", marginBottom: "0.6rem", fontSize: "0.95rem" }}>
        {title}
      </div>

      <div style={{ fontSize: "1.9rem", fontWeight: "bold", color: "#f8fafc" }}>
        {title === "Total Views" ? (
          <AnimatedNumber end={summary?.total_views || 0} duration={2000} separator={true} />
        ) : title === "Total Carts" ? (
          <AnimatedNumber end={summary?.total_carts || 0} duration={2000} separator={true} />
        ) : title === "Total Purchases" ? (
          <AnimatedNumber end={summary?.total_purchases || 0} duration={2000} separator={true} />
        ) : title === "View to Cart Rate" ? (
          <AnimatedNumber
            end={(summary?.view_to_cart_rate || 0) * 100}
            duration={2000}
            decimals={2}
            suffix="%"
          />
        ) : title === "Cart to Purchase Rate" ? (
          <AnimatedNumber
            end={(summary?.cart_to_purchase_rate || 0) * 100}
            duration={2000}
            decimals={2}
            suffix="%"
          />
        ) : title === "Overall Conversion Rate" ? (
          <AnimatedNumber
            end={(summary?.overall_conversion_rate || 0) * 100}
            duration={2000}
            decimals={2}
            suffix="%"
          />
        ) : (
          value
        )}
      </div>
      <div
  style={{
    marginTop: "0.5rem",
    fontSize: "0.9rem",
    color: trend?.startsWith("-") ? "#f87171" : "#4ade80",
    fontWeight: "500",
  }}
>
  {trend}
</div>
    </motion.div>
  );
}

export default function Dashboard() {
  const [summary, setSummary] = useState(null);
  const [funnel, setFunnel] = useState([]);
  const [anomalies, setAnomalies] = useState([]);
  const [aiResponse, setAiResponse] = useState("");
  const [loadingAI, setLoadingAI] = useState(false);

  useEffect(() => {
    API.get("/api/summary")
      .then((res) => setSummary(res.data))
      .catch((err) => console.error("Summary error:", err));

    API.get("/api/funnel")
      .then((res) => setFunnel(res.data))
      .catch((err) => console.error("Funnel error:", err));

    API.get("/api/anomalies")
  .then((res) => {
    const formatted = (res.data || [])
      .map((item) => ({
        date: item.date,
        value: Number(item.z_score),
      }))
      .filter((item) => item.date && !Number.isNaN(item.value))
      .sort((a, b) => new Date(a.date) - new Date(b.date));

    setAnomalies(formatted);
  })
  .catch((err) => console.error("Anomalies error:", err));
  }, []);

  const speak = (text) => {
  if (!text) return;
  const utterance = new SpeechSynthesisUtterance(text);
  speechSynthesis.cancel();
  speechSynthesis.speak(utterance);
};

const handleAskAI = async (question) => {
  console.log("Sending question:", question);

  try {
    setLoadingAI(true);
    const res = await askAssistant(question);
    console.log("API response:", res);

    const reply = res?.reply || "No response received.";
    setAiResponse(reply);
  } catch (err) {
    console.error("AI assistant error:", err);
    setAiResponse("Sorry, I could not connect to the AI assistant.");
  } finally {
    setLoadingAI(false);
  }
}

  const summaryCards = summary
  ? [
      {
        title: "Total Views",
        value: summary.total_views?.toLocaleString(),
        trend: "+12% from last run",
      },
      {
        title: "Total Carts",
        value: summary.total_carts?.toLocaleString(),
        trend: "+8% from last run",
      },
      {
        title: "Total Purchases",
        value: summary.total_purchases?.toLocaleString(),
        trend: "+5% from last run",
      },
      {
        title: "View to Cart Rate",
        value: `${(summary.view_to_cart_rate * 100).toFixed(2)}%`,
        trend: "+2.1% improvement",
      },
      {
        title: "Cart to Purchase Rate",
        value: `${(summary.cart_to_purchase_rate * 100).toFixed(2)}%`,
        trend: "-1.3% drop",
      },
      {
        title: "Overall Conversion Rate",
        value: `${(summary.overall_conversion_rate * 100).toFixed(2)}%`,
        trend: "+0.9% overall",
      },
    ]
  : [];

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "radial-gradient(circle at top, #1e293b 0%, #0f172a 45%, #020617 100%)",
        color: "white",
        fontFamily: "Arial, sans-serif",
      }}
    >
      <HeroGuide
  onWelcome={() =>
    handleAskAI("Give a short welcome introduction for this analytics dashboard")
  }
  onAsk={handleAskAI}
  aiResponse={aiResponse}
  loadingAI={loadingAI}
/>

      <section style={{ padding: "2rem" }}>
        <h2 style={{ marginBottom: "1.2rem", color: "#e2e8f0" }}>Key Metrics</h2>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
            gap: "1rem",
            marginBottom: "2rem",
            background: "radial-gradient(circle at center, rgba(59,130,246,0.1), transparent)",
            padding: "1.5rem",
            borderRadius: "20px",
          }}
        >
          {summaryCards.map((card, index) => (
            <SummaryCard
              key={card.title}
              title={card.title}
              value={card.value}
              trend={card.trend}
              delay={index * 0.08}
              summary={summary}
            />
          ))}
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(420px, 1fr))",
            gap: "1.5rem",
          }}
        >
          <motion.div
            initial={{ opacity: 0, y: 25 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.25 }}
            style={{
              background: "rgba(30, 41, 59, 0.95)",
              borderRadius: "18px",
              padding: "1rem",
              height: "430px",
              boxShadow: "0 8px 30px rgba(0,0,0,0.35)",
              border: "1px solid rgba(148, 163, 184, 0.15)",
            }}
          >
            <h2 style={{ marginBottom: "1rem", color: "#e2e8f0" }}>Funnel Analysis</h2>
            <ResponsiveContainer width="100%" height="90%">
              <BarChart data={funnel}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="step" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="count" />
              </BarChart>
            </ResponsiveContainer>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 25 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.35 }}
            style={{
              background: "rgba(30, 41, 59, 0.95)",
              borderRadius: "18px",
              padding: "1rem",
              height: "430px",
              boxShadow: "0 8px 30px rgba(0,0,0,0.35)",
              border: "1px solid rgba(148, 163, 184, 0.15)",
            }}
          >
            <h2 style={{ marginBottom: "1rem", color: "#e2e8f0" }}>Anomaly Detection</h2>
            <ResponsiveContainer width="100%" height="90%">
              <LineChart data={anomalies}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="value" name="z_score" strokeWidth={2} dot={{ r: 3 }} />
              </LineChart>
            </ResponsiveContainer>
          </motion.div>
        </div>
      </section>

      <ArchitectureSection />
    </div>
  );
}