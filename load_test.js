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
      rate: 10000, // 10k requests per second
      timeUnit: "1s",
      duration: "30s",
      preAllocatedVUs: 1000,
      maxVUs: 1000,
    },
  },
  thresholds: {
    errors: ["rate<0.01"], // <1% error rate
    http_req_duration: ["p(99)<50"], // ensure P99 < 50 ms
  },
};

// Test function
export default function () {
  const payload = JSON.stringify({
    user_id: `user_${Math.floor(Math.random() * 1000000)}`,
    game_id: `game_${Math.floor(Math.random() * 100)}`,
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
