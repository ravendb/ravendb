import { ArrowUpDown, Search } from "lucide-react";
import type { SecurityClearance } from "@/api/custom-services/certificates-service";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/shadcn/ui/input-group";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/shadcn/ui/select";
import {
    CERTIFICATE_STATE_LABELS,
    SECURITY_CLEARANCE_LABELS,
    type CertificateState,
} from "@/pages/dashboard/certificates/certificate-labels";

export type CertificateSort = "name-asc" | "name-desc" | "expiration-asc" | "expiration-desc";

const SORT_OPTIONS: { value: CertificateSort; label: string }[] = [
    { value: "name-asc", label: "Name (A-Z)" },
    { value: "name-desc", label: "Name (Z-A)" },
    { value: "expiration-asc", label: "Expiration (soonest first)" },
    { value: "expiration-desc", label: "Expiration (latest first)" },
];

// UnauthenticatedClients never appears on issued certificates, so it is not offered as a filter.
const FILTERABLE_CLEARANCES: SecurityClearance[] = ["ClusterAdmin", "ClusterNode", "Operator", "ValidUser"];

const CLEARANCE_FILTER_OPTIONS = FILTERABLE_CLEARANCES.map((value) => ({
    value,
    label: SECURITY_CLEARANCE_LABELS[value],
}));

const STATE_FILTER_OPTIONS = Object.entries(CERTIFICATE_STATE_LABELS).map(([value, label]) => ({
    value: value as CertificateState,
    label,
}));

interface CertificatesToolbarProps {
    search: string;
    onSearchChange: (value: string) => void;
    clearance: SecurityClearance | "all";
    onClearanceChange: (value: SecurityClearance | "all") => void;
    state: CertificateState | "all";
    onStateChange: (value: CertificateState | "all") => void;
    sort: CertificateSort;
    onSortChange: (value: CertificateSort) => void;
}

export function CertificatesToolbar({
    search,
    onSearchChange,
    clearance,
    onClearanceChange,
    state,
    onStateChange,
    sort,
    onSortChange,
}: CertificatesToolbarProps) {
    return (
        <div className="flex flex-wrap items-center gap-2">
            <InputGroup className="w-full sm:max-w-xs">
                <InputGroupAddon>
                    <Search />
                </InputGroupAddon>
                <InputGroupInput
                    placeholder="Search by name or thumbprint"
                    value={search}
                    onChange={(event) => onSearchChange(event.target.value)}
                />
            </InputGroup>

            <FilterSelect
                value={clearance}
                onChange={onClearanceChange}
                options={CLEARANCE_FILTER_OPTIONS}
                allLabel="All clearances"
                ariaLabel="Filter by security clearance"
            />
            <FilterSelect
                value={state}
                onChange={onStateChange}
                options={STATE_FILTER_OPTIONS}
                allLabel="All states"
                ariaLabel="Filter by state"
            />

            <Select value={sort} onValueChange={(value) => onSortChange(value as CertificateSort)}>
                <SelectTrigger aria-label="Sort certificates" className="w-auto sm:ml-auto">
                    <ArrowUpDown aria-hidden="true" />
                    <SelectValue />
                </SelectTrigger>
                <SelectContent align="end">
                    {SORT_OPTIONS.map((option) => (
                        <SelectItem key={option.value} value={option.value}>
                            {option.label}
                        </SelectItem>
                    ))}
                </SelectContent>
            </Select>
        </div>
    );
}

function FilterSelect<T extends string>({
    value,
    onChange,
    options,
    allLabel,
    ariaLabel,
}: {
    value: T | "all";
    onChange: (value: T | "all") => void;
    options: { value: T; label: string }[];
    allLabel: string;
    ariaLabel: string;
}) {
    return (
        <Select value={value} onValueChange={(next) => onChange(next as T | "all")}>
            <SelectTrigger aria-label={ariaLabel} className="w-auto max-w-48 min-w-32">
                <SelectValue />
            </SelectTrigger>
            <SelectContent>
                <SelectItem value="all">{allLabel}</SelectItem>
                {options.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                        {option.label}
                    </SelectItem>
                ))}
            </SelectContent>
        </Select>
    );
}
