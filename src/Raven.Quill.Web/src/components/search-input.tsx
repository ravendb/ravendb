import { useRef } from "react";
import { Search, X } from "lucide-react";
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupInput } from "@/components/shadcn/ui/input-group";

interface SearchInputProps {
    value: string;
    onValueChange: (value: string) => void;
    placeholder: string;
    clearLabel: string;
    "aria-label"?: string;
    className?: string;
}

export function SearchInput({
    value,
    onValueChange,
    placeholder,
    clearLabel,
    "aria-label": ariaLabel,
    className,
}: SearchInputProps) {
    const inputRef = useRef<HTMLInputElement>(null);

    const clear = () => {
        onValueChange("");
        inputRef.current?.focus();
    };

    return (
        <InputGroup className={className}>
            <InputGroupAddon>
                <Search aria-hidden="true" />
            </InputGroupAddon>
            <InputGroupInput
                ref={inputRef}
                type="search"
                aria-label={ariaLabel}
                placeholder={placeholder}
                value={value}
                className="[&::-webkit-search-cancel-button]:hidden"
                onChange={(event) => onValueChange(event.target.value)}
                onKeyDown={(event) => {
                    if (event.key === "Escape" && value) {
                        event.preventDefault();
                        clear();
                    }
                }}
            />
            {value && (
                <InputGroupAddon align="inline-end">
                    <InputGroupButton size="icon-xs" aria-label={clearLabel} onClick={clear}>
                        <X aria-hidden="true" />
                    </InputGroupButton>
                </InputGroupAddon>
            )}
        </InputGroup>
    );
}
