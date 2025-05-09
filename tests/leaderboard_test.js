import http from "k6/http";
import { check } from "k6";
import { Rate } from "k6/metrics";

// Custom metrics
const errorRate = new Rate("errors");

// Get API URL from environment variable
const API_URL = __ENV.API_URL || "http://host.docker.internal:8000";

// Test configuration
export const options = {
  scenarios: {
    leaderboard_test: {
      executor: "constant-arrival-rate",
      rate: 100, // 100 requests per second
      timeUnit: "1s",
      duration: "10s",
      preAllocatedVUs: 100,
      maxVUs: 100,
    },
  },
  thresholds: {
    errors: ["rate<0.01"], // <1% error rate
    http_req_duration: ["p(99)<200"], // ensure P99 < 200 ms
  },
};

// Test function
export default function () {
  const gameId = "game_1"; // Using the same game_id as in the load test
  const response = http.get(`${API_URL}/games/${gameId}/leaders`);

  check(response, {
    "status is 200": (r) => r.status === 200,
    "response has correct structure": (r) => {
      const body = r.json();
      return (
        Array.isArray(body) &&
        body.every(
          (entry) =>
            typeof entry.user_id === "string" &&
            typeof entry.score === "number" &&
            typeof entry.rank === "number" &&
            typeof entry.percentile === "number" &&
            entry.percentile >= 0 &&
            entry.percentile <= 100
        )
      );
    },
  });

  errorRate.add(response.status !== 200);
}
