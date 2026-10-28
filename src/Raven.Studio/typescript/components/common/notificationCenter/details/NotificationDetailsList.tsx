import { ReactNode } from "react";

export function NotificationDetailsList({ children }: { children: ReactNode }) {
    return <dl className="notification-details-list">{children}</dl>;
}

export function NotificationDetailsListItem({ label, children }: { label: ReactNode; children: ReactNode }) {
    return (
        <>
            <dt>{label}</dt>
            <dd>{children}</dd>
        </>
    );
}
