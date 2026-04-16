import { motion } from "framer-motion";

const nodes = [
  {
    title: "Data Sources",
    text: "Events, transactions, product catalog, and customer behavior logs enter the analytics pipeline.",
  },
  {
    title: "PySpark Pipeline",
    text: "Distributed preprocessing, sessionization, funnel analysis, attribution, and anomaly detection run on the dataset.",
  },
  {
    title: "FastAPI Layer",
    text: "Processed frontend-ready JSON is exposed through clean API endpoints for the dashboard.",
  },
  {
    title: "React Dashboard",
    text: "Interactive visual analytics portal for metrics, charts, explanations, and the 3D guide experience.",
  },
];

export default function ArchitectureSection() {
  return (
    <section style={{ padding: "0 2rem 2rem 2rem" }}>
      <h2 style={{ color: "#e2e8f0", marginBottom: "1.2rem" }}>System Architecture</h2>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))",
          gap: "1rem",
        }}
      >
        {nodes.map((node, index) => (
          <motion.div
            key={node.title}
            initial={{ opacity: 0, y: 25 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.45, delay: index * 0.08 }}
            style={{
              background: "rgba(30, 41, 59, 0.95)",
              borderRadius: "18px",
              padding: "1.2rem",
              border: "1px solid rgba(148, 163, 184, 0.15)",
              boxShadow: "0 8px 30px rgba(0,0,0,0.35)",
            }}
          >
            <h3 style={{ marginBottom: "0.7rem", color: "#f8fafc" }}>{node.title}</h3>
            <p style={{ color: "#cbd5e1", lineHeight: 1.6 }}>{node.text}</p>
          </motion.div>
        ))}
      </div>
    </section>
  );
}