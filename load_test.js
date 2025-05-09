import http from "k6/http";
import { check } from "k6";
import { Rate } from "k6/metrics";

// Custom metrics
const errorRate = new Rate("errors");

// Test configuration
export const options = {
  scenarios: {
    throughput_test: {
      executor: "constant-arrival-rate",
      rate: 100,
      timeUnit: "1s", // per second
      duration: "10s", // for 1 minute
      preAllocatedVUs: 1000,
      maxVUs: 1000,
    },
  },
  thresholds: {
    errors: ["rate<0.1"], // <10% error rate
    http_req_duration: ["p(99)<50"], // ensure P99 < 50 ms
  },
};

// Helpers
function generateRandomScore() {
  return Math.floor(Math.random() * 10000);
}
function generateRandomUserId() {
  return `user_${Math.floor(Math.random() * 100000)}`;
}
function generateRandomGameId() {
  return `game_${Math.floor(Math.random() * 100)}`;
}

export default function () {
  const baseUrl = __ENV.API_URL || "http://app:8000";

  const payload = {
    user_id: generateRandomUserId(),
    game_id: generateRandomGameId(),
    score: generateRandomScore(),
    timestamp: Date.now() / 1000, // epoch seconds
  };

  const params = {
    headers: { "Content-Type": "application/json" },
    tags: { endpoint: "ingest", game_id: payload.game_id },
  };

  const res = http.post(`${baseUrl}/ingest`, JSON.stringify(payload), params);

  const ok = check(res, {
    "status is 201": (r) => r.status === 201,
    "body.status is ok": (r) => {
      try {
        return JSON.parse(r.body).status === "success";
      } catch {
        return false;
      }
    },
  });

  if (!ok) {
    errorRate.add(1);
  }
}
