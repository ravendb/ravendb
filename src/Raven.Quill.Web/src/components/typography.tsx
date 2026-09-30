/* eslint-disable react-refresh/only-export-components */
import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/utils";

// The app's single source of truth for heading and body-text styling. Prefer these over hand-written
// `text-*`/`font-*` classes so the scale stays consistent.
//
// For a section header (title, optional description, optional action) prefer <SectionHeader> from
// @/components/section-header — it picks `as`+`variant` from one semantic `level` so the same role
// can't render at two sizes across call sites. Reach for <Heading> directly only for standalone titles.
//
// <Heading> — pick `as` for the document outline (the tag), `variant` for the look; they are independent
// because the same visual level appears at different depths across the app. Keep one <h1> per page and
// don't skip levels (h1 → h2 → h3).
//   page       h1 page / wizard-step title
//   title      xl standalone-screen title with no page shell (auth, error/empty states)
//   section    h2 section header
//   subsection h3 sub-section header
//   label      compact chrome / card-item / eyebrow header (semantically an h2/h3, visually small)
//
// <Text> — supporting copy. `body` (plain), `muted` (secondary), `caption` (xs metadata), `label`
// (emphasized inline). Renders <p> by default.
//
// Card/overlay titles are their own primitives kept visually aligned with this scale: CardTitle,
// DialogTitle, SheetTitle, AlertDialogTitle all use text-base font-semibold tracking-tight. Use
// `<CardTitle asChild><h2>…</h2></CardTitle>` when a card is a genuine page section.

type HeadingElement = "h1" | "h2" | "h3" | "h4" | "h5" | "h6";

const headingVariants = cva("text-foreground", {
    variants: {
        variant: {
            page: "text-2xl font-semibold tracking-tight",
            title: "text-xl font-semibold tracking-tight",
            section: "text-lg font-semibold tracking-tight",
            subsection: "text-base font-semibold",
            label: "text-sm font-semibold",
        },
    },
    defaultVariants: {
        variant: "section",
    },
});

function Heading({
    className,
    variant = "section",
    as = "h2",
    ...props
}: React.ComponentPropsWithoutRef<HeadingElement> &
    VariantProps<typeof headingVariants> & {
        as?: HeadingElement;
    }) {
    const Comp = as;

    return (
        <Comp
            data-slot="heading"
            data-variant={variant}
            className={cn(headingVariants({ variant }), className)}
            {...props}
        />
    );
}

const textVariants = cva("", {
    variants: {
        variant: {
            body: "text-sm",
            muted: "text-sm text-muted-foreground",
            caption: "text-xs text-muted-foreground",
            label: "text-sm font-medium",
        },
    },
    defaultVariants: {
        variant: "body",
    },
});

type TextElement = "p" | "span" | "div";

type TextVariant = NonNullable<VariantProps<typeof textVariants>["variant"]>;

function Text({
    className,
    variant = "body",
    as = "p",
    ...props
}: React.ComponentPropsWithoutRef<TextElement> &
    VariantProps<typeof textVariants> & {
        as?: TextElement;
    }) {
    const Comp = as;

    return (
        <Comp data-slot="text" data-variant={variant} className={cn(textVariants({ variant }), className)} {...props} />
    );
}

export { Heading, headingVariants, Text, textVariants, type TextVariant };
