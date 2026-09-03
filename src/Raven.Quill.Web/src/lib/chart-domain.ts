import type { AxisDomainItem } from "recharts";

// Recharts centers a flat series when the domain collapses to [0, 0]; keeping the upper
// bound above zero makes all-zero data rest on the bottom baseline like other charts.
export const ZERO_SAFE_Y_DOMAIN: [AxisDomainItem, AxisDomainItem] = [0, (dataMax: number) => Math.max(dataMax, 1)];
