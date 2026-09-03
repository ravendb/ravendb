import { Monitor, Moon, Sun, type LucideIcon } from "lucide-react";
import type { Theme } from "@/components/shadcn/theme-provider";

export const THEME_OPTIONS: { value: Theme; label: string; icon: LucideIcon }[] = [
    { value: "light", label: "Light", icon: Sun },
    { value: "system", label: "System", icon: Monitor },
    { value: "dark", label: "Dark", icon: Moon },
];
