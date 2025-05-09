import http from "k6/http";
import { check } from "k6";
import { Rate } from "k6/metrics";

// Custom metrics
const errorRate = new Rate("errors");

// Test configuration
export const options = {
  scenarios: {
    latency_test: {
      executor: "ramping-arrival-rate",
      startRate: 100, // Start with 100 requests per second
      timeUnit: "1s",
      preAllocatedVUs: 100,
      maxVUs: 100,
      stages: [
        { duration: "30s", target: 100 }, // Stay at 100 RPS for 30s
        { duration: "30s", target: 500 }, // Ramp up to 500 RPS
        { duration: "30s", target: 1000 }, // Ramp up to 1000 RPS
        { duration: "30s", target: 2000 }, // Ramp up to 2000 RPS
        { duration: "30s", target: 0 }, // Ramp down to 0
      ],
    },
  },
  thresholds: {
    errors: ["rate<0.01"], // <1% error rate
    http_req_duration: ["p(50)<10", "p(90)<50", "p(95)<100", "p(99)<200"],
  },
};

// Test function
export default function () {
  const payload = JSON.stringify({
    user_id: `user_${Math.floor(Math.random() * 100)}`,
    game_id: `game_1`,
    score: Math.floor(Math.random() * 1000000),
  });

  const params = {
    headers: {
      "Content-Type": "application/json",
    },
  };

  const response = http.post(
    "http://host.docker.internal:8000/ingest",
    payload,
    params
  );

  check(response, {
    "status is 201": (r) => r.status === 201,
    "response has success status": (r) => r.json().status === "success",
  });

  errorRate.add(response.status !== 201);
}
