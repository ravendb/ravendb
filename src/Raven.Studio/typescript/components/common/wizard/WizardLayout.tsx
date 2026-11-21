import "./WizardLayout.scss";
import { FormEventHandler, ReactNode } from "react";
import classNames from "classnames";

interface WizardLayoutProps {
    content: ReactNode;
    footer: ReactNode;
    sidebar: ReactNode;
    onSubmit?: FormEventHandler<HTMLFormElement>;
    className?: string;
}

export default function WizardLayout({ content, footer, sidebar, onSubmit, className }: WizardLayoutProps) {
    return (
        <form onSubmit={onSubmit} className={classNames("wizard-layout", className)} autoComplete="off">
            <div className="wizard-layout-main wizard-content">{content}</div>
            <div className="wizard-layout-footer">{footer}</div>
            <div className="wizard-layout-sidebar wizard-sidebar">{sidebar}</div>
        </form>
    );
}
