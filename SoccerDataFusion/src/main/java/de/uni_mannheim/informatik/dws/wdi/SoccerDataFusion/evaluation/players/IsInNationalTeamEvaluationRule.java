/*
 * Copyright (c) 2017 Data and Web Science Group, University of Mannheim, Germany (http://dws.informatik.uni-mannheim.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.evaluation.players;

import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Player;
import de.uni_mannheim.informatik.dws.winter.datafusion.EvaluationRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;

/**
 * {@link EvaluationRule} for the shirt number of national team of {@link Player}s. The rule simply
 * compares the shirt number of two {@link Player}s and returns true, in case they
 * are identical.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class IsInNationalTeamEvaluationRule extends EvaluationRule<Player, Attribute> {

	@Override
	public boolean isEqual(Player record1, Player record2, Attribute schemaElement) {
		if(record1.getIsInNationalTeam()== null && record2.getIsInNationalTeam()==null)
			return true;
		else if(record1.getIsInNationalTeam()== null ^ record2.getIsInNationalTeam()==null)
			return false;
		else if (record1.getIsInNationalTeam().equals(record2.getIsInNationalTeam()))
			return true;
		else
			return false;
		
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.datafusion.EvaluationRule#isEqual(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.Correspondence)
	 */
	@Override
	public boolean isEqual(Player record1, Player record2,
			Correspondence<Attribute, Matchable> schemaCorrespondence) {
		return isEqual(record1, record2, (Attribute)null);
	}
	
}
