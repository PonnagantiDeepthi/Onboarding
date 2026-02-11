package ug.daes.onboarding.exceptions;

import java.util.Locale;

import org.hibernate.PessimisticLockException;
import org.hibernate.QueryTimeoutException;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.DataException;
import org.hibernate.exception.GenericJDBCException;
import org.hibernate.exception.JDBCConnectionException;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.SQLGrammarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.MessageSource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;

import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.util.AppUtil;
import ug.daes.onboarding.util.Utility;

@Component
public class ExceptionHandlerUtil extends Exception {

	private static final String CLASS = ExceptionHandlerUtil.class.getSimpleName();
	final static Logger logger = LoggerFactory.getLogger(ExceptionHandlerUtil.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private MessageSource messageSource;

	public ExceptionHandlerUtil(MessageSource messageSource) {
		this.messageSource = messageSource;
	}

	public static ApiResponse handleException(Exception e) {
		String errorMessage = "Something went wrong. Please try again later.";

		String errorCode = ErrorCodeException.GENERIC_ERROR.getCode(); // Default error code

		// Handle specific SQL-related exceptions
		if (e instanceof JDBCConnectionException) {
			errorCode = ErrorCodeException.CONNECTION_ERROR.getCode();
			logger.error("{} - {} : Database connection error occurred: {}", CLASS, Utility.getMethodName(),
					e.getMessage());
		} else if (e instanceof ConstraintViolationException) {
			errorCode = ErrorCodeException.DATABASE_ERROR.getCode();
			logger.error("{} - {} : Constraint violation error occurred: {}", CLASS, Utility.getMethodName(),
					e.getMessage());
		} else if (e instanceof DataException || e instanceof LockAcquisitionException
				|| e instanceof PessimisticLockException || e instanceof QueryTimeoutException
				|| e instanceof SQLGrammarException || e instanceof GenericJDBCException) {
			errorCode = ErrorCodeException.DATABASE_ERROR.getCode();
			logger.error("{} - {} : Database-related error occurred: {}", CLASS, Utility.getMethodName(),
					e.getMessage());
		} else {
			logger.error("{} - {} : An unexpected error occurred: {}", CLASS, Utility.getMethodName(), e.getMessage());
		}

		// Add the error code to the message
		String formattedMessage = String.format("%s [ErrorCode: %s]", errorMessage, errorCode);

		// Log the response being returned
		logger.info("{} - {} : Returning error response: errorCode={}, message={}", CLASS, Utility.getMethodName(),
				errorCode, formattedMessage);

		// Return the error response with the formatted message
		return createErrorResponse(errorCode, formattedMessage);
	}

	public ApiResponse handleHttpException(Exception e) {
		String errorCode;
		String errorMessage;

		if (e instanceof HttpStatusCodeException) {
			HttpStatusCodeException httpEx = (HttpStatusCodeException) e;
			HttpStatus status = HttpStatus.valueOf(httpEx.getRawStatusCode());

			// Map HTTP status codes to custom error codes
			switch (status) {
			case BAD_REQUEST:
				errorCode = ErrorCodeException.BAD_REQUEST.getCode();
				errorMessage = ErrorCodeException.BAD_REQUEST.getMessage();
				break;
			case UNAUTHORIZED:
				errorCode = ErrorCodeException.REST_CLIENT_ERROR.getCode();
				errorMessage = ErrorCodeException.REST_CLIENT_ERROR.getMessage();
				break;
			case FORBIDDEN:
				errorCode = ErrorCodeException.REST_CLIENT_ERROR.getCode();
				errorMessage = ErrorCodeException.REST_CLIENT_ERROR.getMessage();
				break;
			case NOT_FOUND:
				errorCode = ErrorCodeException.REST_CLIENT_ERROR.getCode();
				errorMessage = ErrorCodeException.REST_CLIENT_ERROR.getMessage();
				break;
			case INTERNAL_SERVER_ERROR:
				errorCode = ErrorCodeException.INTERNAL_SERVER_ERROR.getCode();
				errorMessage = ErrorCodeException.INTERNAL_SERVER_ERROR.getMessage();
				break;
			case SERVICE_UNAVAILABLE:
				errorCode = ErrorCodeException.SERVICE_UNAVAILABLE.getCode();
				errorMessage = ErrorCodeException.SERVICE_UNAVAILABLE.getMessage();
				break;
			case GATEWAY_TIMEOUT:
				errorCode = ErrorCodeException.SERVICE_UNAVAILABLE.getCode();
				errorMessage = ErrorCodeException.SERVICE_UNAVAILABLE.getMessage();
				break;
			default:
				// For other HTTP status codes that we don't explicitly handle
				errorCode = ErrorCodeException.UNKNOWN_ERROR.getCode();
				errorMessage = ErrorCodeException.UNKNOWN_ERROR.getMessage();
			}

			// Format the response message for HTTP error
			String formattedMessage = String.format("HTTP Error: %s - %s (%s)", status.value(), errorMessage,
					errorCode);
			logger.error("{} - {} : HTTP exception occurred: status={}, errorCode={}, message={}", CLASS,
					Utility.getMethodName(), status.value(), errorCode, e.getMessage());
			logger.info("{} - {} : Returning HTTP error response: errorCode={}, message={}", CLASS,
					Utility.getMethodName(), errorCode, formattedMessage);
			return AppUtil.createApiResponse(false, formattedMessage, null);
		} else if (e instanceof ResourceAccessException) {
			// Handle network issues such as timeouts, connection errors, etc.
			errorCode = ErrorCodeException.REST_CONNECTION_ERROR.getCode();
			errorMessage = ErrorCodeException.REST_CONNECTION_ERROR.getMessage();

			// Format the response message for network error
			String formattedMessage = String.format("Network Error: %s (%s)", errorMessage, errorCode);
			logger.error("{} - {} : Network exception occurred: message={}", CLASS, Utility.getMethodName(),
					e.getMessage());
			logger.info("{} - {} : Returning network error response: errorCode={}, message={}", CLASS,
					Utility.getMethodName(), errorCode, formattedMessage);
			return AppUtil.createApiResponse(false, formattedMessage, null);
		} else {
			// Handle other unexpected exceptions
			errorCode = ErrorCodeException.UNKNOWN_ERROR.getCode();
			errorMessage = ErrorCodeException.UNKNOWN_ERROR.getMessage();

			// Generic error message for unexpected exceptions
			String formattedMessage = String.format("Unexpected Error: %s (%s)", errorMessage, errorCode);
			logger.error("{} - {} : Unexpected exception occurred: message={}", CLASS, Utility.getMethodName(),
					e.getMessage());
			logger.info("{} - {} : Returning unexpected error response: errorCode={}, message={}", CLASS,
					Utility.getMethodName(), errorCode, formattedMessage);
			return AppUtil.createApiResponse(false, formattedMessage, null);
		}
	}

	public OnboardingExecption onboardingServiceException(String messageKey) {
		String message = messageSource.getMessage(messageKey, null, Locale.ENGLISH);
		return new OnboardingExecption(message);
	}

	// Method to create error response
	public static ApiResponse createErrorResponse(String errorCode, String errorMessage) {
		ApiResponse response = new ApiResponse();
		response.setSuccess(false);
		response.setMessage(errorMessage);
		response.setResult(null);
		return response;
	}

	// You can also add a method to create success responses if needed
	public ApiResponse createSuccessResponse(String successMessage, Object result) {
		ApiResponse response = new ApiResponse();
		String successMeg = messageSource.getMessage(successMessage, null, Locale.ENGLISH);
		response.setSuccess(true); // Indicates success
		response.setMessage(successMeg); // Set success message
		response.setResult(result); // Set the result data
		return response;
	}

	// You can also add a method to create success responses if needed
	public ApiResponse createSuccessResponseWithCustomMessage(String successMessage, Object result) {
		ApiResponse response = new ApiResponse();
		//String successMeg = messageSource.getMessage(successMessage, null, Locale.ENGLISH);
		response.setSuccess(true); // Indicates success
		response.setMessage(successMessage); // Set success message
		response.setResult(result); // Set the result data
		return response;
	}
	
	
	public ApiResponse createFailedResponseWithCustomMessage(String successMessage, Object result) {
		ApiResponse response = new ApiResponse();
		response.setSuccess(false);
		response.setMessage(successMessage); 
		response.setResult(result);
		return response;
	}

	public ApiResponse successResponse(String successMessage) {
		ApiResponse response = new ApiResponse();
		String successMeg = messageSource.getMessage(successMessage, null, Locale.ENGLISH);
		response.setSuccess(true); // Indicates success
		response.setMessage(successMeg); // Set success message
		response.setResult(null); // Set the result data
		return response;
	}

	// You can also add a method to create success responses if needed
	public ApiResponse createErrorResponseWithResult(String successMessage, Object result) {
		ApiResponse response = new ApiResponse();
		String successMeg = messageSource.getMessage(successMessage, null, Locale.ENGLISH);
		response.setSuccess(false); // Indicates success
		response.setMessage(successMeg); // Set success message
		response.setResult(result); // Set the result data
		return response;
	}

	/**
	 * Utility method to create a generic error response with a localized message.
	 * 
	 * @param messageKey The key to fetch the localized message.
//	 * @param locale     The locale to be used for message translation.
	 * @return The API response containing the localized error message.
	 */
	public ApiResponse createErrorResponse(String messageKey) {
		String errorMessage = messageSource.getMessage(messageKey, null, Locale.ENGLISH);
		// Log the error message
		logger.error("Error response created with message: {}", errorMessage);
		// Return the response with the message and default error code
		return AppUtil.createApiResponse(false, errorMessage, null);
	}

	public ApiResponse handleErrorRestTemplateResponse(int statusCode) {
		switch (statusCode) {
		case 500:
			return AppUtil.createApiResponse(false,
					messageSource.getMessage("api.error.internal.server.error", null, Locale.ENGLISH), statusCode);
		case 400:
			return AppUtil.createApiResponse(false,
					messageSource.getMessage("api.error.bad.request", null, Locale.ENGLISH), null);
		case 401:
			return AppUtil.createApiResponse(false,
					messageSource.getMessage("api.error.unauthorized", null, Locale.ENGLISH), null);
		case 403:
			return AppUtil.createApiResponse(false,
					messageSource.getMessage("api.error.forbidden", null, Locale.ENGLISH), null);
		case 408:
			return AppUtil.createApiResponse(false,
					messageSource.getMessage("api.error.request.timeout", null, Locale.ENGLISH), null);
		default:
			return AppUtil.createApiResponse(false, messageSource.getMessage(
					"api.error.something.went.wrong.please.try.after.sometime", null, Locale.ENGLISH), null);
		}
	}

}
