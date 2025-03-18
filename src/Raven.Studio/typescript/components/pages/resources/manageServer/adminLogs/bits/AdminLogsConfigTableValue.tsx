import { Icon } from "components/common/Icon";

export default function AdminLogsConfigTableValue({ value }: { value: unknown }) {
    if (value == null) {
        return <Icon icon="minus" color="muted" title="null" />;
    }
    if (value === false) {
        return <Icon icon="cancel" color="danger" title="false" />;
    }
    if (value === true) {
        return <Icon icon="check" color="success" title="true" />;
    }

    return String(value);
}
