namespace Raven.Quill.Channels;

/// <summary>
/// How a web-widget (iFrame) embed page is styled: one of the two built-in presets
/// (<see cref="Light"/> / <see cref="Dark"/>) or operator-authored <see cref="Custom"/> CSS.
/// Travels on the wire as its string name (global <c>JsonStringEnumConverter</c>), like
/// <see cref="ChannelType"/>. Inheritance and legacy-doc fallbacks live in
/// <see cref="IFrameStyleResolution"/>.
/// </summary>
public enum IFrameStyle
{
    Light,
    Dark,
    Custom,
}
