import http from "k6/http";
import { check } from "k6";
import { Rate } from "k6/metrics";

// Custom metrics
const errorRate = new Rate("errors");

// Test configuration
export const options = {
  scenarios: {
    read_stress_test: {
      executor: "constant-arrival-rate",
      rate: 5000, // 5k requests per second
      timeUnit: "1s",
      duration: "30s",
      preAllocatedVUs: 1000,
      maxVUs: 1000,
    },
  },
  thresholds: {
    errors: ["rate<0.001"], // <0.1% error rate
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
