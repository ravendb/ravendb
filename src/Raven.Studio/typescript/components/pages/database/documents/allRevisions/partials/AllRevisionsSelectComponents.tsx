import { SelectValue, SelectOption } from "components/common/select/Select";
import { components, OptionProps, SingleValueProps } from "react-select";
import Badge from "react-bootstrap/Badge";

export interface SelectOptionWithCount<T extends SelectValue = string> extends SelectOption<T> {
    count: number;
}

export function OptionWithCount(props: OptionProps<SelectOptionWithCount>) {
    const { data } = props;

    return (
        <div className="cursor-pointer">
            <components.Option {...props}>
                {data.label}
                {data.count != null && (
                    <Badge className="ms-1" bg="secondary">
                        {data.count.toLocaleString()}
                    </Badge>
                )}
            </components.Option>
        </div>
    );
}

export function SingleValueWithCount({ children, ...props }: SingleValueProps<SelectOptionWithCount>) {
    const { data } = props;

    return (
        <components.SingleValue {...props}>
            {children}
            {data.count != null && (
                <Badge className="ms-1" bg="secondary">
                    {data.count.toLocaleString()}
                </Badge>
            )}
        </components.SingleValue>
    );
}
