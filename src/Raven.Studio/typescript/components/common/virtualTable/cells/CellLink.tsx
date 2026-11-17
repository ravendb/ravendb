import { PropsWithChildren } from "react";

interface CellLinkProps extends PropsWithChildren {
    href: string;
}

export default function CellLink({ href, children }: CellLinkProps) {
    return (
        <a href={href} className="cell-link no-decor">
            <span className="cell-link-text">{children}</span>
        </a>
    );
}
