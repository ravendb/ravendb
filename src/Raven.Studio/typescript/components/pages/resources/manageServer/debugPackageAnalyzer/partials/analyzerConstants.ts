export const analyzerConstants = {
    minRowsForControls: 10,
    // Analyzer tables sit inside a Bootstrap `.p-4` panel (1.5rem = 24px each side), but their
    // SizeGetter measures the panel's outer width. Subtract this when sizing columns so a table fits
    // the inner area instead of overflowing it by ~2px (which would show a horizontal scrollbar).
    panelHorizontalPaddingInPx: 48,
};
