using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Client.Documents.AI;

public interface IAiConversationOperations
{
    /// <summary>
    /// Registers an asynchronous handler for an action tool.
    /// </summary>
    /// <typeparam name="TArgs">The type of the argument passed to the handler.</typeparam>
    /// <param name="actionName">The name of the action tool to handle.</param>
    /// <param name="action">A function that processes the arguments and returns a <see cref="Task{Object}"/> representing the response.</param>
    /// <param name="aiHandleError">An optional strategy for handling errors during execution.</param>
    void Handle<TArgs>(string actionName, Func<TArgs, Task<object>> action, AiHandleErrorStrategy aiHandleError = AiHandleErrorStrategy.Default) where TArgs : class;

    /// <summary>
    /// Registers a synchronous handler for an action tool.
    /// </summary>
    /// <typeparam name="TArgs">The type of the argument passed to the handler.</typeparam>
    /// <param name="actionName">The name of the action tool to handle.</param>
    /// <param name="action">A function that processes the arguments and returns the result.</param>
    /// <param name="aiHandleError">An optional strategy for handling errors during execution.</param>
    void Handle<TArgs>(string actionName, Func<TArgs, object> action, AiHandleErrorStrategy aiHandleError = AiHandleErrorStrategy.Default) where TArgs : class;

    /// <summary>
    /// Asynchronously executes one “turn” of the conversation:  
    /// sends the current prompt, processes any required actions,  
    /// and awaits the agent’s reply.
    /// </summary>
    /// <typeparam name="TAnswer">The expected type of the content response.</typeparam>
    /// <param name="token">
    /// A <see cref="CancellationToken"/> used to cancel the operation.
    /// </param>
    /// <returns>
    /// A <see cref="Task{AiAnswer}"/> containing an <see cref="AiAnswer{TAnswer}"/>  
    /// indicating the outcome of the turn:
    /// <list type="bullet">
    /// <item><see cref="AiConversationResult.ActionRequired"/> if the agent requires further interaction (e.g., pending tool requests).</item>
    /// <item><see cref="AiConversationResult.Done"/> if the conversation has completed and a final answer is available.</item>
    /// </list>
    /// </returns>
    Task<AiAnswer<TAnswer>> RunAsync<TAnswer>(CancellationToken token = default);

    /// <summary>
    /// Synchronously executes one turn of the conversation.
    /// </summary>
    /// <typeparam name="TAnswer">The expected type of the content response.</typeparam>
    /// <returns>
    /// An <see cref="AiAnswer{TAnswer}"/> indicating the outcome of the turn:
    /// <list type="bullet">
    /// <item><see cref="AiConversationResult.ActionRequired"/> if more interaction is needed (e.g., pending tool requests).</item>
    /// <item><see cref="AiConversationResult.Done"/> if the conversation has completed and a final answer is available.</item>
    /// </list>
    /// </returns>
    AiAnswer<TAnswer> Run<TAnswer>();

    /// <summary>
    /// The identifier of this conversation.
    /// </summary>
    string Id { get; }

    /// <summary>
    /// The current RavenDB change vector for this conversation document,  
    /// used to detect concurrent modifications.
    /// </summary>
    string ChangeVector { get; }

    /// <summary>
    /// Retrieves the list of action-tool requests  
    /// that the AI agent needs you to execute.
    /// </summary>
    /// <returns>
    /// A sequence of <see cref="AiAgentActionRequest"/>  
    /// representing the tools the agent asked for.
    /// </returns>
    IEnumerable<AiAgentActionRequest> RequiredActions();

    /// <summary>
    /// Adds a string response for a given action request.
    /// </summary>
    /// <param name="actionId">
    /// The identifier of the action request.
    /// </param>
    /// <param name="actionResponse">
    /// The textual response to supply to the agent.
    /// </param>
    void AddActionResponse(string actionId, string actionResponse);

    /// <summary>
    /// Adds a typed response object for a given action request.
    /// </summary>
    /// <typeparam name="TResponse">
    /// The CLR type of the response object.  
    /// Must be a reference type.
    /// </typeparam>
    /// <param name="actionId">
    /// The identifier of the action request.
    /// </param>
    /// <param name="actionResponse">
    /// The response object to supply to the agent.
    /// </param>
    void AddActionResponse<TResponse>(string actionId, TResponse actionResponse) where TResponse : class;

    /// <summary>
    /// Sets the next user prompt to send to the AI agent.
    /// </summary>
    /// <param name="userPrompt">
    /// The text of the user’s message.
    /// </param>
    void SetUserPrompt(string userPrompt);
}

public enum AiHandleErrorStrategy
{
    Default,
    SendErrorsToModel,
    RaiseImmediately
}
