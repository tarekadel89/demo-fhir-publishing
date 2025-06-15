package ca.uhn.fhir.jpa.starter.ips;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.ips.api.IIpsGenerationStrategy;
import ca.uhn.fhir.jpa.ips.generator.IIpsGeneratorSvc;
import ca.uhn.fhir.jpa.ips.jpa.DefaultJpaIpsGenerationStrategy;
import ca.uhn.fhir.jpa.ips.provider.IpsOperationProvider;
import ca.uhn.fhir.jpa.starter.custom.CustomIpsGenerator;

import org.springframework.context.annotation.Bean;

import org.springframework.context.annotation.Configuration;

@Configuration
public class StarterIpsConfig {
	@Bean
	IIpsGenerationStrategy ipsGenerationStrategy() {
		return new DefaultJpaIpsGenerationStrategy();
	}

	@Bean
	public IpsOperationProvider ipsOperationProvider(IIpsGeneratorSvc theIpsGeneratorSvc) {
		return new IpsOperationProvider(theIpsGeneratorSvc);
	}

	@Bean
	public CustomIpsGenerator customIpsGenerator(
			FhirContext theFhirContext, IIpsGenerationStrategy theGenerationStrategy) {
		return new CustomIpsGenerator(theFhirContext, theGenerationStrategy);
	}
}
