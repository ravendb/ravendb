import { useAppSelector } from "components/store";
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";
import { aiAssistantSelectors } from "../shell/aiAssistantSlice";

interface AiAssistantUsagePercentageCircleProps {
    size?: number;
    strokeWidth?: number;
}

export default function AiAssistantUsagePercentageCircle({
    size = 14,
    strokeWidth = 1,
}: AiAssistantUsagePercentageCircleProps) {
    const usage = useAppSelector(aiAssistantSelectors.usage);

    const canShow = usage.status === "success" && usage.data?.Status === "Success";
    if (!canShow) {
        return null;
    }

    const radius = (size - strokeWidth) / 2;
    const circumference = 2 * Math.PI * radius;
    const center = size / 2;

    const usagePercentage = usage.data.UsagePercentage;
    const strokeDasharray = circumference;
    const strokeDashoffset = circumference * (1 - usagePercentage / 100);

    const formattedPercentage = `${usagePercentage.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 })}%`;

    return (
        <PopoverWithHoverWrapper message={`${formattedPercentage} tokens used this month.`}>
            <div className="d-flex align-items-center gap-1">
                <svg
                    width={size}
                    height={size}
                    version="1.1"
                    xmlns="http://www.w3.org/2000/svg"
                    style={{ transform: "rotate(-90deg)" }}
                >
                    <circle
                        r={radius}
                        cx={center}
                        cy={center}
                        fill="transparent"
                        stroke="var(--panel-bg-3)"
                        strokeWidth={strokeWidth}
                    />
                    <circle
                        r={radius}
                        cx={center}
                        cy={center}
                        stroke="var(--text-emphasis)"
                        strokeWidth={strokeWidth}
                        strokeLinecap="butt"
                        strokeDashoffset={strokeDashoffset}
                        fill="transparent"
                        strokeDasharray={strokeDasharray}
                    />
                </svg>
                <span className="fs-5 fw-light">{formattedPercentage}</span>
            </div>
        </PopoverWithHoverWrapper>
    );
}
