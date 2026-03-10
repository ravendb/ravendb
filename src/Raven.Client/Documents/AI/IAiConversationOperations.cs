using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Client.Documents.AI;

/// <summary>
/// Fluent API for handling tool calls and running a conversation with an AI agent.
/// Implementations allow registering handlers/receivers for tools, adding responses,
/// setting the user prompt, and executing conversation turns.
/// </summary>
public interface IAiConversationOperations
{
    /// <summary>
    /// Registers an asynchronous handler for an action tool.
    /// </summary>
    /// <typeparam name="TArgs">The type of the argument passed to the handler.</typeparam>
    /// <typeparam name="TResult">The type of the result returned by the handler.</typeparam>
    /// <param name="actionName">The name of the action tool to handle.</param>
    /// <param name="action">A function that processes the arguments and returns a <see cref="Task{Object}"/> representing the response.</param>
    /// <param name="aiHandleError">An optional strategy for handling errors during execution.</param>
    void Handle<TArgs, TResult>(string actionName, Func<TArgs, Task<TResult>> action, AiHandleErrorStrategy aiHandleError = AiHandleErrorStrategy.SendErrorsToModel)
        where TArgs : class
        where TResult : class;

    /// <summary>
    /// Registers a synchronous handler for an action tool.
    /// </summary>
    /// <typeparam name="TArgs">The type of the argument passed to the handler.</typeparam>
    /// <param name="actionName">The name of the action tool to handle.</param>
    /// <param name="action">A function that processes the arguments and returns the result.</param>
    /// <param name="aiHandleError">An optional strategy for handling errors during execution.</param>
    void Handle<TArgs>(string actionName, Func<TArgs, object> action, AiHandleErrorStrategy aiHandleError = AiHandleErrorStrategy.SendErrorsToModel) where TArgs : class;

    /// <summary>
    /// Registers an asynchronous handler for an action tool.
    /// </summary>
    /// <typeparam name="TArgs">The type of the argument passed to the handler.</typeparam>
    /// <typeparam name="TResult">The type of the result returned by the handler.</typeparam>
    /// <param name="actionName">The name of the action tool to handle.</param>
    /// <param name="action">A function that processes the arguments and returns a <see cref="Task{Object}"/> representing the response.</param>
    /// <param name="aiHandleError">An optional strategy for handling errors during execution.</param>
    void Handle<TArgs, TResult>(string actionName, Func<AiAgentActionRequest, TArgs, Task<TResult>> action, AiHandleErrorStrategy aiHandleError = AiHandleErrorStrategy.SendErrorsToModel)
        where TArgs : class
        where TResult : class;

    /// <summary>
    /// Registers a synchronous handler for an action tool.
    /// </summary>
    /// <typeparam name="TArgs">The type of the argument passed to the handler.</typeparam>
    /// <param name="actionName">The name of the action tool to handle.</param>
    /// <param name="action">A function that processes the arguments and returns the result.</param>
    /// <param name="aiHandleError">An optional strategy for handling errors during execution.</param>
    void Handle<TArgs>(string actionName, Func<AiAgentActionRequest, TArgs, object> action, AiHandleErrorStrategy aiHandleError = AiHandleErrorStrategy.SendErrorsToModel) where TArgs : class;

    /// <summary>
    /// Registers an asynchronous receiver for an action tool - unlike handlers, receivers can *act* on the call
    /// from the model, but require an explicit call to <see cref="AddActionResponse"/> to close the action.
    /// The <see cref="AddActionResponse"/> call may happen at a later time (including using a different <see cref="IAiConversationOperations"/>
    /// instance (such as a separate request at a later time). 
    /// </summary>
    /// <typeparam name="TArgs">The type of the argument passed to the handler.</typeparam>
    /// <param name="actionName">The name of the action tool to handle.</param>
    /// <param name="action">A function that processes the arguments and returns the result.</param>
    /// <param name="aiHandleError">An optional strategy for handling errors during execution.</param>
    void Receive<TArgs>(string actionName, Func<AiAgentActionRequest, TArgs, Task> action, AiHandleErrorStrategy aiHandleError = AiHandleErrorStrategy.SendErrorsToModel)
        where TArgs : class;

    /// <summary>
    /// Registers a synchronous receiver for an action tool - unlike handlers, receivers can *act* on the call
    /// from the model, but require an explicit call to <see cref="AddActionResponse"/> to close the action.
    /// The <see cref="AddActionResponse"/> call may happen at a later time (including using a different <see cref="IAiConversationOperations"/>
    /// instance (such as a separate request at a later time). 
    /// </summary>
    /// <typeparam name="TArgs">The type of the argument passed to the handler.</typeparam>
    /// <param name="actionName">The name of the action tool to handle.</param>
    /// <param name="action">A function that processes the arguments and returns the result.</param>
    /// <param name="aiHandleError">An optional strategy for handling errors during execution.</param>
    void Receive<TArgs>(string actionName, Action<AiAgentActionRequest, TArgs> action, AiHandleErrorStrategy aiHandleError = AiHandleErrorStrategy.SendErrorsToModel)
        where TArgs : class;

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
    /// Asynchronously executes one “turn” of the conversation, streaming the specified property's value for immediate feedback.
    /// Sends the current prompt, processes any required actions, and awaits the agent’s reply while invoking the callback with streamed values.
    /// </summary>
    /// <typeparam name="TAnswer">The expected type of the content response.</typeparam>
    /// <param name="streamPropertyPath">The property of the response to stream.</param>
    /// <param name="streamedChunksCallback">
    /// A callback function invoked with streamed value of the specified property.
    /// Must be a simple string property, *strongly* recommended that it would be the first one defined in the schema.
    /// </param>
    /// <param name="token">A <see cref="CancellationToken"/> used to cancel the operation.</param>
    /// <returns>
    /// A <see cref="Task{AiAnswer}"/> containing an <see cref="AiAnswer{TAnswer}"/> indicating the outcome of the turn:
    /// <list type="bullet">
    /// <item><see cref="AiConversationResult.ActionRequired"/> if the agent requires further interaction (e.g., pending tool requests).</item>
    /// <item><see cref="AiConversationResult.Done"/> if the conversation has completed and a final answer is available.</item>
    /// </list>
    /// </returns>
    Task<AiAnswer<TAnswer>> StreamAsync<TAnswer>(string streamPropertyPath, Func<string, Task> streamedChunksCallback, CancellationToken token = default);

    /// <summary>
    /// Asynchronously executes one “turn” of the conversation, streaming the specified property's value for immediate feedback.
    /// Sends the current prompt, processes any required actions, and awaits the agent’s reply while invoking the callback with streamed values.
    /// </summary>
    /// <typeparam name="TAnswer">The expected type of the content response.</typeparam>
    /// <param name="streamPropertyPath">The property of the response to stream.</param>
    /// <param name="streamedChunksCallback">
    /// A callback function invoked with streamed value of the specified property.
    /// Must be a simple string property, *strongly* recommended that it would be the first one defined in the schema.
    /// </param>
    /// <param name="token">A <see cref="CancellationToken"/> used to cancel the operation.</param>
    /// <returns>
    /// A <see cref="Task{AiAnswer}"/> containing an <see cref="AiAnswer{TAnswer}"/> indicating the outcome of the turn:
    /// <list type="bullet">
    /// <item><see cref="AiConversationResult.ActionRequired"/> if the agent requires further interaction (e.g., pending tool requests).</item>
    /// <item><see cref="AiConversationResult.Done"/> if the conversation has completed and a final answer is available.</item>
    /// </list>
    /// </returns>
    Task<AiAnswer<TAnswer>> StreamAsync<TAnswer>(Expression<Func<TAnswer, string>> streamPropertyPath, Func<string, Task> streamedChunksCallback,
        CancellationToken token = default);

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
    /// <param name="toolId">
    /// The identifier of the action request.
    /// </param>
    /// <param name="actionResponse">
    /// The textual response to supply to the agent.
    /// </param>
    void AddActionResponse(string toolId, string actionResponse);

    /// <summary>
    /// Adds a typed response object for a given action request.
    /// </summary>
    /// <typeparam name="TResponse">
    /// The CLR type of the response object.  
    /// Must be a reference type.
    /// </typeparam>
    /// <param name="toolId">
    /// The identifier of the action request.
    /// </param>
    /// <param name="actionResponse">
    /// The response object to supply to the agent.
    /// </param>
    void AddActionResponse<TResponse>(string toolId, TResponse actionResponse) where TResponse : class;

    /// <summary>
    /// Injects an artificial action (tool call) and a string response into the model's conversation context.
    /// This is an advanced mechanism to programmatically prompt the agent, causing it to "believe" 
    /// it successfully executed a tool and received the specified <paramref name="actionResponse"/>.
    /// </summary>
    /// <param name="toolId">
    /// The name of the tool to simulate the agent called.
    /// </param>
    /// <param name="actionResponse">
    /// The string response to supply to the agent as the result of the simulated action.
    /// </param>
    void AddArtificialActionWithResponse(string toolId, string actionResponse);


    /// <summary>
    /// Injects an artificial action (tool call) and a typed response object into the model's conversation context.
    /// This allows for sophisticated programmatic prompting by making the agent "believe" 
    /// it successfully executed a tool and received a structured <paramref name="actionResponse"/>.
    /// </summary>
    /// <typeparam name="TResponse">
    /// The CLR type of the response object. Must be a reference type.
    /// </typeparam>
    /// <param name="toolId">
    /// The name of the tool to simulate the agent called.
    /// </param>
    /// <param name="actionResponse">
    /// The response object to supply to the agent as the result of the simulated action.
    /// </param>
    void AddArtificialActionWithResponse<TResponse>(string toolId, TResponse actionResponse) where TResponse : class;

    /// <summary>
    /// Sets the next user prompt to send to the AI agent.
    /// </summary>
    /// <param name="userPrompt">
    /// The text of the user’s message.
    /// </param>
    void SetUserPrompt(string userPrompt);

    /// <summary>
    /// Adds the next user prompt or prompts to send to the AI agent.
    /// </summary>
    /// <param name="userPrompt">
    /// The text of the user’s message.
    /// </param>
    void AddUserPrompt(params IEnumerable<string> userPrompt);

    /// <summary>
    /// Adds a file attachment as a stream to the conversation turn.
    /// </summary>
    /// <param name="name">The name of the attachment (e.g., ""monthly_budget.pdf").
    /// A descriptive name is highly recommended as it helps the LLM understand the file's context and content.</param>
    /// <param name="stream">The data stream of the file.</param>
    /// <param name="contentType">The MIME media type of the attachment content (e.g. image/png).</param>
    void AddAttachment(string name, Stream stream, string contentType);

    /// <summary>
    /// Copies an existing attachment from a document in RavenDB into the conversation context.
    /// </summary>
    /// <param name="sourceDocumentId">The ID of the document in RavenDB that contains the attachment.</param>
    /// <param name="fileName">The name to assign to the file in the conversation context.</param>
    void CopyAttachmentFrom(string sourceDocumentId, string fileName);

    /// <summary>
    /// This is called if the model invoked an action that has no register handler using
    /// <see cref="Handle"/> or <see cref="Receive"/>. If there is no event handler for
    /// this event and an unexpected action is raised by the model, and exception will
    /// be thrown.
    ///
    /// You can also access all the actions that require answers using the <see cref="RequiredActions"/> method.
    /// </summary>
    event Func<UnhandledActionEventArgs, Task> OnUnhandledAction;
}

/// <summary>
/// Specifies how errors thrown by tool handlers/receivers should be handled.
/// </summary>
public enum AiHandleErrorStrategy
{
    /// <summary>
    /// Convert the error to a textual message and send it back to the model as the tool response.
    /// </summary>
    SendErrorsToModel,

    /// <summary>
    /// Throw the exception immediately to the caller instead of sending it to the model.
    /// </summary>
    RaiseImmediately
}

/// <summary>
/// Event arguments for <see cref="IAiConversationOperations.OnUnhandledAction"/>.
/// </summary>
public class UnhandledActionEventArgs
{
    internal UnhandledActionEventArgs(IAiConversationOperations sender, AiAgentActionRequest action, CancellationToken token)
    {
        Sender = sender;
        Action = action;
        Token = token;
    }

    /// <summary>
    /// The sender that raised the event.
    /// </summary>
    public IAiConversationOperations Sender { get; private set; }

    /// <summary>
    /// The action (tool call) requested by the AI model that requires handling.
    /// </summary>
    public AiAgentActionRequest Action { get; private set; }

    /// <summary>
    /// Cancellation token associated with the current turn.
    /// </summary>
    public CancellationToken Token { get; private set; }
}
