package scap2rdf;

import org.eclipse.emf.ecore.EObject;

import gov.nist.scap.schema.asset.reporting.format._1.AssetReportCollectionType;
import gov.nist.scap.schema.asset.reporting.format._1.AssetType;
import gov.nist.scap.schema.asset.reporting.format._1.ReportType;
import gov.nist.scap.schema.asset.reporting.format._1._1Package;
import gov.nist.scap.schema.asset.reporting.format._1.util._1Switch;

public class SCAPSwitch extends _1Switch<Boolean> {

	@Override
	protected Boolean doSwitch(int classifierID, EObject theEObject) {
		switch (classifierID) {
		case _1Package.ASSET_REPORT_COLLECTION_TYPE: {
			AssetReportCollectionType assetReportCollectionType = (AssetReportCollectionType) theEObject;
			Boolean result = caseAssetReportCollectionType(assetReportCollectionType);
			if (result == null)
				result = caseRelationshipsContainerType(assetReportCollectionType);
			if (result == null)
				result = defaultCase(theEObject);
			return result;
		}
		case _1Package.ASSET_TYPE: {
			AssetType assetType = (AssetType)theEObject;
			Boolean result = caseAssetType(assetType);
			if (result == null) result = defaultCase(theEObject);
			return result;
		}
		default:
			return defaultCase(theEObject);
		}
	}

	@Override
	public Boolean caseAssetReportCollectionType(AssetReportCollectionType object) {
		// TODO Auto-generated method stub
		return super.caseAssetReportCollectionType(object);
	}

	@Override
	public Boolean caseAssetType(AssetType object) {
		// TODO Auto-generated method stub
		return super.caseAssetType(object);
	}

	@Override
	public Boolean caseReportType(ReportType object) {
		// TODO Auto-generated method stub
		return super.caseReportType(object);
	}
}
