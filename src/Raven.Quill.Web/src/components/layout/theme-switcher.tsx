import { useTheme, type Theme } from "@/components/shadcn/theme-provider";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuRadioGroup,
    DropdownMenuRadioItem,
    DropdownMenuTrigger,
} from "@/components/shadcn/ui/dropdown-menu";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { THEME_OPTIONS } from "@/lib/theme-options";
import { cn } from "@/lib/utils";

/**
 * Theme control with two layouts:
 *
 * - `inline` (expanded sidebar): a segmented toggle that hides the inactive
 *   options until hover/focus, so the row collapses to the active button's
 *   width at rest.
 *
 * - `dropdown` (collapsed rail): a single icon button showing the active
 *   theme's glyph that opens a menu of the three options. The hover-expand
 *   trick has no room in a narrow rail, so we fall back to a menu.
 */
export function ThemeSwitcher({ variant = "inline" }: { variant?: "inline" | "dropdown" }) {
    const { theme, setTheme } = useTheme();

    if (variant === "dropdown") {
        const ActiveIcon = (THEME_OPTIONS.find((option) => option.value === theme) ?? THEME_OPTIONS[1]).icon;
        return (
            <DropdownMenu>
                <Tooltip>
                    <TooltipTrigger asChild>
                        <DropdownMenuTrigger
                            aria-label="Theme"
                            className={cn(
                                "flex size-8 items-center justify-center rounded-md text-sidebar-foreground/70 transition-colors",
                                "hover:bg-sidebar-foreground/8 hover:text-sidebar-foreground",
                                "focus-visible:ring-2 focus-visible:ring-ring/60 focus-visible:outline-none",
                            )}
                        >
                            <ActiveIcon className="size-4" aria-hidden="true" />
                        </DropdownMenuTrigger>
                    </TooltipTrigger>
                    <TooltipContent side="right">Theme</TooltipContent>
                </Tooltip>
                <DropdownMenuContent side="right" align="end" className="w-36">
                    <DropdownMenuRadioGroup value={theme} onValueChange={(value) => setTheme(value as Theme)}>
                        {THEME_OPTIONS.map(({ value, label, icon: Icon }) => (
                            <DropdownMenuRadioItem key={value} value={value}>
                                <Icon className="size-4" aria-hidden="true" />
                                <span>{label}</span>
                            </DropdownMenuRadioItem>
                        ))}
                    </DropdownMenuRadioGroup>
                </DropdownMenuContent>
            </DropdownMenu>
        );
    }

    return (
        <div
            role="radiogroup"
            aria-label="Theme"
            className={cn(
                "group relative inline-flex items-center gap-0.5 overflow-hidden rounded-md",
                "border border-transparent transition-colors",
                "hover:bg-sidebar-foreground/8/40 hover:border-border",
                "focus-within:border-border focus-within:bg-accent/40",
            )}
        >
            {THEME_OPTIONS.map(({ value, label, icon: Icon }) => {
                const isActive = theme === value;
                return (
                    <button
                        key={value}
                        type="button"
                        role="radio"
                        aria-checked={isActive}
                        aria-label={label}
                        title={label}
                        onClick={() => setTheme(value)}
                        className={cn(
                            "relative flex h-8 shrink-0 items-center justify-center rounded-md transition-all duration-200 ease-out",
                            "focus-visible:ring-2 focus-visible:ring-ring/60 focus-visible:outline-none",
                            isActive
                                ? "w-8 text-foreground"
                                : cn(
                                      "pointer-events-none w-0 max-w-0 opacity-0",
                                      "group-hover:pointer-events-auto group-hover:w-8 group-hover:max-w-8 group-hover:opacity-100",
                                      "group-focus-within:pointer-events-auto group-focus-within:w-8 group-focus-within:max-w-8 group-focus-within:opacity-100",
                                      "text-muted-foreground hover:text-foreground",
                                  ),
                        )}
                    >
                        <Icon className="size-4" aria-hidden="true" />
                    </button>
                );
            })}
        </div>
    );
}
