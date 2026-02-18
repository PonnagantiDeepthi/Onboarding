package ug.daes.onboarding.controller;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.dto.TemporaryTableDTO;
import ug.daes.onboarding.dto.UpdateTemporaryTableDto;
import ug.daes.onboarding.service.iface.ProposedFlowIface;
import ug.daes.onboarding.util.Utility;

@RestController
public class ProposedFlowController {

	private static Logger logger = LoggerFactory.getLogger(ProposedFlowController.class);
	/** The Constant CLASS. */
	final static String CLASS = "ProposedFlowColtroller";

	@Autowired
	private ProposedFlowIface proposedFlowIface;

	ObjectMapper mapper = new ObjectMapper();

	@PostMapping(value = "api/save/temporary-data")
	public ApiResponse saveDataTemporaryTable(HttpServletRequest request, @RequestBody TemporaryTableDTO model)
			throws JsonProcessingException {
		logger.info("{}{} - Request received with idDocNumber: {}", CLASS, Utility.getMethodName(),
				model.getIdDocNumber());
		return proposedFlowIface.saveDataTemporyTable(model);
	}

	// @PostMapping(value = "api/save/video-selfie/details/temporary-data")
//	@PostMapping(value = "api/save/video-selfie/details/temporary-data", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
//	public ApiResponse saveStep2DataTemporaryTable(@RequestParam(value = "file", required = false) MultipartFile file,
//			@RequestParam(value = "selfie", required = false) String selfie,
//			@RequestParam(value = "model") String model) throws JsonProcessingException {
//		TemporaryTableDTO temporaryTableDTO = mapper.readValue(model, TemporaryTableDTO.class);
//		logger.info("{}{} - Request received with idDocNumber: {}", CLASS, Utility.getMethodName(),
//				temporaryTableDTO.getIdDocNumber());
//		return proposedFlowIface.saveStep2Details(temporaryTableDTO, file, selfie);
//	}

//	@PostMapping(value = "api/save/video-selfie/details/temporary-data", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
//	public ApiResponse saveStep2DataTemporaryTable(@RequestParam(value = "selfie", required = false) String selfie,
//			@RequestParam(value = "model") String model) throws JsonProcessingException {
//		TemporaryTableDTO temporaryTableDTO = mapper.readValue(model, TemporaryTableDTO.class);
//		logger.info("{}{} - Request received with idDocNumber: {}", CLASS, Utility.getMethodName(),
//				temporaryTableDTO.getIdDocNumber());
//		return proposedFlowIface.saveStep2Details(temporaryTableDTO, null, selfie);
//	}

	@PostMapping(value = "api/save/video-selfie/details/temporary-data", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public ApiResponse saveStep2DataTemporaryTable(@RequestPart("model") String model,
			@RequestPart(value = "selfie", required = false) String selfie,
			@RequestPart(value = "file", required = false) MultipartFile file) throws JsonProcessingException {
		TemporaryTableDTO temporaryTableDTO = mapper.readValue(model, TemporaryTableDTO.class);
		logger.info("Multipart received - model length: {}, selfie length: {}", model != null ? model.length() : 0,
				selfie != null ? selfie.length() : 0);
		logger.info("{}{} - Request received with idDocNumber: {}", CLASS, Utility.getMethodName(),
				temporaryTableDTO.getIdDocNumber());
		return proposedFlowIface.saveStep2Details(temporaryTableDTO, null, selfie);
	}

	@PostMapping(value = "api/submit/ob-data/{idDocumentNumber}")
	public ApiResponse submitData(@PathVariable String idDocumentNumber) {
		logger.info("{}{} - Request received with idDocumentNumber: {}", CLASS, Utility.getMethodName(),
				idDocumentNumber);
		return proposedFlowIface.submitObData(idDocumentNumber);
	}



	@PostMapping(value = "api/update/temporaryTable")
	public ApiResponse updateRecordByDeviceIdOrMobileOrEmail(
			@RequestBody UpdateTemporaryTableDto updateTemporaryTableDto) {
		logger.info("{}{} - Request received with idDocNumber: {}", CLASS, Utility.getMethodName(),
				updateTemporaryTableDto.getIdDocNumber());
		return proposedFlowIface.updateRecord(updateTemporaryTableDto);
	}

//    @PostMapping(value = "api/delete/record/{anyValue}")
//    public ApiResponse deleteRecordbyDeviceIdorMobNoOrEmail(@PathVariable String anyValue){
//
//        return proposedFlowIface.deleteRecord(anyValue);
//    }

	@PostMapping(value = "api/delete-record/temporaryTable")
	public ApiResponse deleteRecordbyDeviceIdorMobNoOrEmail(
			@RequestBody UpdateTemporaryTableDto updateTemporaryTableDto) {
		logger.info(CLASS + " inside deleteRecordbyDeviceIdorMobNoOrEmail()");
		return proposedFlowIface.deleteRecord(updateTemporaryTableDto);
	}

//    @Scheduled(cron = "0 0 0 * * ?")
//    public void deleteOldRecords() {
//        // Get the current time
//        LocalDateTime now = LocalDateTime.now();
//
//        // Fetch all records from the repository
//        List<TemporaryTable> records = temporaryTableRepo.findAll();
//
//        for (TemporaryTable record : records) {
//            // Check if the updated time is older than or equal to 24 hours
//            if (record.getUpdatedOn().isBefore(now.minusHours(24))) {
//                myEntityRepository.delete(record);
//                System.out.println("Deleted record with ID: " + record.getId());
//            }
//        }

	@PostMapping(value = "api/extract/features")
	public ApiResponse getAllSubscriberExtractFeatures() {
		logger.info(CLASS + " inside getAllSubscriberExtractFeatures()");
		return proposedFlowIface.getAllSubscriberExtractFeatures();
	}

	@PostMapping(value = "/api/encripted-string")
	public ApiResponse encriptedString(@RequestBody TemporaryTableDTO encriptedString) {
		return proposedFlowIface.encriptedString(encriptedString);
	}

	@GetMapping("/nira-response/{docNumber}")
	public ApiResponse getNiraResponse(@PathVariable String docNumber) {
		return proposedFlowIface.niraResponse(docNumber);
	}

}
