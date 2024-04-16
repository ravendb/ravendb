
type TodoType = "Sharding" | "Styling" | "Feature" | "BugFix" | "Limits" | "Other";

type TeamMember = "Marcin" | "Danielle" | "Kwiato" | "Damian" | "Matteo" | "ANY";

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function todo(feature: TodoType, member: TeamMember, message: string, issueUrl?: string) {
    // empty
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function shardingTodo(member: TeamMember = "ANY", message = "TODO", issueUrl?: string) {
    // empty
}
