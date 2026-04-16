import axios from "axios";

const API = axios.create({
  baseURL: "http://127.0.0.1:8000",
});

export const askAssistant = async (message) => {
  const response = await API.post("/api/chat", { message });
  return response.data;
};

export default API;