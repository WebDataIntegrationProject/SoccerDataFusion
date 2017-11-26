package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import de.uni_mannheim.informatik.dws.winter.model.FusibleFactory;
import de.uni_mannheim.informatik.dws.winter.model.RecordGroup;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;

public class FusibleClubFactory implements FusibleFactory<Club, Attribute> {

	@Override
	public Club createInstanceForFusion(RecordGroup<Club, Attribute> cluster) {

		List<String> ids = new LinkedList<>();

		for (Club c : cluster.getRecords()) {
			ids.add(c.getIdentifier());
		}

		Collections.sort(ids);

		String mergedId = StringUtils.join(ids, '+');

		return new Club(mergedId, "fused");
	}
	
}
