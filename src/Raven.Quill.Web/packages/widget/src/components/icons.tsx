// Hand-rolled so the widget carries no icon library. Every icon inherits `currentColor` and sizes
// from the `size-*` class its caller sets.
type IconProps = { className?: string };

const BASE_PROPS = {
    viewBox: "0 0 24 24",
    fill: "none",
    stroke: "currentColor",
    strokeWidth: 2,
    strokeLinecap: "round",
    strokeLinejoin: "round",
    "aria-hidden": true,
} as const;

export function ArrowUpIcon({ className }: IconProps) {
    return (
        <svg {...BASE_PROPS} className={className}>
            <path d="M12 19V5" />
            <path d="m5 12 7-7 7 7" />
        </svg>
    );
}

export function StopIcon({ className }: IconProps) {
    return (
        <svg {...BASE_PROPS} className={className} fill="currentColor" strokeWidth={0}>
            <rect x="7" y="7" width="10" height="10" rx="1.5" />
        </svg>
    );
}

export function CopyIcon({ className }: IconProps) {
    return (
        <svg {...BASE_PROPS} className={className}>
            <rect x="9" y="9" width="11" height="11" rx="2" />
            <path d="M5 15V5a2 2 0 0 1 2-2h8" />
        </svg>
    );
}

export function CheckIcon({ className }: IconProps) {
    return (
        <svg {...BASE_PROPS} className={className}>
            <path d="m4 12 5 5L20 6" />
        </svg>
    );
}

export function ArrowDownIcon({ className }: IconProps) {
    return (
        <svg {...BASE_PROPS} className={className}>
            <path d="M12 5v14" />
            <path d="m19 12-7 7-7-7" />
        </svg>
    );
}
