using Raven.Quill.Contracts;

namespace Raven.Quill.Wizard;

public sealed record WizardDraftSaveRequest(
    string AppName,
    SelectedSourceTable[] SelectedTables,
    string Step,
    string Slug = "");
