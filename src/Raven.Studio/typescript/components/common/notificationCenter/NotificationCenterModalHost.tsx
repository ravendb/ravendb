import { createElement } from "react";
import { useKnockoutValue } from "hooks/useKnockoutValue";

interface NotificationCenterModalHostProps {
    modalView: KnockoutObservable<ReactInKnockout<any>>;
}

export default function NotificationCenterModalHost({ modalView }: NotificationCenterModalHostProps) {
    const view = useKnockoutValue(modalView);

    return view ? <ModalView view={view} /> : null;
}

function ModalView({ view }: { view: ReactInKnockout<any> }) {
    const { component, props } = useKnockoutValue(view);

    return createElement(component, props);
}
