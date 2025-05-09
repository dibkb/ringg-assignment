import http from "k6/http";
import { check } from "k6";
import { Rate } from "k6/metrics";

// Custom metrics
const errorRate = new Rate("errors");

// Test configuration
export const options = {
  scenarios: {
    read_latency_test: {
      executor: "ramping-arrival-rate",
      startRate: 50, // Start with 50 requests per second
      timeUnit: "1s",
      preAllocatedVUs: 50,
      maxVUs: 50,
      stages: [
        { duration: "30s", target: 50 }, // Stay at 50 RPS for 30s
        { duration: "30s", target: 200 }, // Ramp up to 200 RPS
        { duration: "30s", target: 500 }, // Ramp up to 500 RPS
        { duration: "30s", target: 1000 }, // Ramp up to 1000 RPS
        { duration: "30s", target: 0 }, // Ramp down to 0
      ],
    },
  },
  thresholds: {
    errors: ["rate<0.01"], // <1% error rate
    http_req_duration: [
      "p(50)<10", // 50% of requests should be below 10ms
      "p(90)<20", // 90% of requests should be below 20ms
      "p(95)<30", // 95% of requests should be below 30ms
      "p(99)<50", // 99% of requests should be below 50ms
    ],
  },
};

// Test function
export default function () {
  const gameId = "game_1"; // Using the same game_id as in the load test
  const limit = 10;
  const params = {
    headers: {
      "Content-Type": "application/json",
    },
  };

  const response = http.get(
    `http://host.docker.internal:8000/games/${gameId}/leaders?limit=${limit}`,
    params
  );

  check(response, {
    "status is 200": (r) => r.status === 200,
    "response has correct structure": (r) => {
      const body = r.json();
      const { entries } = body;
      return entries.length == limit;
    },
  });

  errorRate.add(response.status !== 200);
}
