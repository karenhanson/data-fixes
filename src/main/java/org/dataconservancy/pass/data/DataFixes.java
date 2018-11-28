package org.dataconservancy.pass.data;
import java.io.FileOutputStream;

import java.net.URI;

import java.util.Date;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import org.dataconservancy.pass.client.PassClient;
import org.dataconservancy.pass.client.PassClientFactory;
import org.dataconservancy.pass.client.fedora.FedoraConfig;
import org.dataconservancy.pass.model.Deposit;
import org.dataconservancy.pass.model.File;
import org.dataconservancy.pass.model.Repository;
import org.dataconservancy.pass.model.RepositoryCopy;
import org.dataconservancy.pass.model.Submission;
import org.dataconservancy.pass.model.SubmissionEvent;

/*
 * Copyright 2018 Johns Hopkins University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Data fixes
 */
public class DataFixes {

    private final static String PASS_BASE_URL = "http://localhost:8080/fcrepo/rest/";
    private final static String PASS_ELASTICSEARCH_URL = "http://localhost:9090/pass/"; 
    private final static String PASS_FEDORA_USER = "fedoraAdmin";
    private final static String PASS_FEDORA_PASSWORD = "moo";
    private final static String PASS_SEARCH_LIMIT = "5000";

    private static PassClient client;
    
    static {
        // Hardcoding these things in
        System.setProperty("pass.fedora.baseurl", PASS_BASE_URL);
        System.setProperty("pass.elasticsearch.url", PASS_ELASTICSEARCH_URL);
        System.setProperty("pass.fedora.user", PASS_FEDORA_USER);
        System.setProperty("pass.fedora.password", PASS_FEDORA_PASSWORD);
        System.setProperty("pass.elasticsearch.limit", PASS_SEARCH_LIMIT);
        client = PassClientFactory.getPassClient();
    }

    static final java.io.File dumpDir = new java.io.File("dump-" + new Date().getTime());

    static final java.io.File deletedDir = new java.io.File(dumpDir, "deleted");

    static final java.io.File editedDir = new java.io.File(dumpDir, "edited");
    
    static CloseableHttpClient http;

    
    public static void main(String[] args) {
        
        try {
            //comment these in if you are using dump files
            
            http = getAuthClient();
            editedDir.mkdirs();
            deletedDir.mkdirs();
            System.out.println("Dumping deleted resources in " + deletedDir.getAbsolutePath());
            System.out.println("Dumping resources prior to editing in " + editedDir.getAbsolutePath());

            //comment out once done, but maintain for the record
            //updateJScholarshipRepoFormSchema();   
            //removeExcessTestSubmission(); 
            
            
        } catch (Exception ex)  {
            System.err.println("Update failed: " + ex.getMessage());
        }
    }

    private static void removeExcessTestSubmission() throws Exception {
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/7d/2c/6b/27/7d2c6b27-991a-42b4-ae1b-0ef5ac0f470e"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/2f/c3/ea/a1/2fc3eaa1-8a3f-41c1-b9fc-63c7e050aaf7"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/ea/21/1b/06/ea211b06-16c6-4ecd-9d0c-f513beee6d05"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/f5/68/fa/c7/f568fac7-092e-4c33-8337-54c60e451f19"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/74/11/08/fa/741108fa-926f-46f6-ad88-75cbcc23ba9c"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/e6/c0/c4/0a/e6c0c40a-d56e-4813-a39c-a48f4b7921bd"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/1b/e2/a9/0b/1be2a90b-69d9-452a-8549-9e95e097052f"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/5d/27/ab/2a/5d27ab2a-76af-4800-89e8-cc125d3ba997"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/8d/9d/a7/45/8d9da745-e7fc-401b-a9ea-5360697c0a5f"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/fc/7d/ca/3a/fc7dca3a-1ac8-4cea-b8de-c4b66bd3aac7"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/04/4c/0a/2e/044c0a2e-43fa-4611-889a-36fd7c104161"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/7f/c6/ff/dd/7fc6ffdd-2529-4d3c-a365-991eee0e4170"));
        //deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/97/fd/e3/36/97fde336-bba5-40dc-8b1e-e695cfab3593"));
        deleteSubmission(new URI("http://localhost:8080/fcrepo/rest/submissions/2c/86/32/f2/2c8632f2-7b26-4f50-9a64-777577e92d02"));
        
    }
    
    private static void deleteSubmission(URI submissionId) throws Exception {

        Submission submission = client.readResource(submissionId, Submission.class);
        
        Set<URI> depositIds = client.findAllByAttribute(Deposit.class, "submission", submissionId);
        depositIds.forEach(delete);
        System.out.println(String.format("Deleted %s Deposits for Submission %s.", depositIds.size(), submissionId));
        
        Set<URI> fileIds = client.findAllByAttribute(File.class, "submission", submissionId);
        fileIds.forEach(delete);
        System.out.println(String.format("Deleted %s Files for Submission %s.", fileIds.size(), submissionId));
        
        Set<URI> submissionEventsIds = client.findAllByAttribute(SubmissionEvent.class, "submission", submissionId);
        submissionEventsIds.forEach(delete);
        System.out.println(String.format("Deleted %s SubmissionEvents for Submission %s.", submissionEventsIds.size(), submissionId));
        
        //check if other submissions reference same publication. if not can delete the pub and repocopies
        Set<URI> pubSubmissionIds = client.findAllByAttribute(Submission.class, "publication", submission.getPublication());
        if (pubSubmissionIds.size()==1) {
            Set<URI> repositoryCopyIds = client.findAllByAttribute(RepositoryCopy.class, "publication", submission.getPublication());
            repositoryCopyIds.forEach(delete);
            System.out.println(String.format("Deleted %s RepositoryCopies for Submission %s.", repositoryCopyIds.size(), submissionId));
            client.deleteResource(submission.getPublication());
            System.out.println(String.format("Deleted Publication %s for Submission %s.", submission.getPublication(), submissionId));
        } else {
            System.out.println(String.format("Did not delete any RepositoryCopy or Publication records for the Submission %s as they may be referenced in other Submissions", submissionId));
        }
        
        delete.accept(submissionId);        
    }
        
    private static void updateJScholarshipRepoFormSchema() throws Exception {

        URI jscholarshipRepoUri = new URI("http://localhost:8080/fcrepo/rest/repositories/41/96/0a/92/41960a92-d3f8-4616-86a6-9e9cadc1a269");
        Repository repository = client.readResource(jscholarshipRepoUri, Repository.class);
        
        String formSchema = "{"  
                    + "\"id\":\"JScholarship\","
                    + "\"schema\":{"
                       + "\"title\":\"Deposit Details - JScholarship <br><p class='lead text-muted'>Please provide the information and agreement required for JScholarship deposit.</p> <p class='lead text-muted'> <i class='glyphicon glyphicon-info-sign'></i> Fields that are not editable were populated using information associated with the provided DOI or were provided in a previous step. To edit authors list, please go back to the &#34;Publication Details&#34; page via the &#34;Back&#34; button below.</p>\","
                       + "\"type\":\"object\","
                       + "\"properties\":{"
                          + "\"authors\":{"
                             + "\"title\":\"<div class='row'><div class='col-6'>Author(s) <small class='text-muted'>(required)</small></div><div class='col-6 p-0'></div></div>\","
                             + "\"type\":\"array\","
                             + "\"uniqueItems\":true,"
                             + "\"items\":{"
                                 + "\"type\":\"object\","
                                 + "\"properties\":{"
                                     + "\"author\":{"
                                         + "\"type\":\"string\","
                                         + "\"fieldClass\":\"body-text col-6 pull-left pl-0\""
                                   + "}"
                                + "}"
                             + "}"
                          + "},"
                          + "\"embargo\":{"
                             + "\"type\":\"string\","
                             + "\"default\":\"NON-EXCLUSIVE LICENSE FOR USE OF MATERIALS This non-exclusive license defines the terms for the deposit of Materials in all formats into the digital repository of materials collected, preserved and made available through the Johns Hopkins Digital Repository, JScholarship. The Contributor hereby grants to Johns Hopkins a royalty free, non-exclusive worldwide license to use, re-use, display, distribute, transmit, publish, re-publish or copy the Materials, either digitally or in print, or in any other medium, now or hereafter known, for the purpose of including the Materials hereby licensed in the collection of materials in the Johns Hopkins Digital Repository for educational use worldwide. In some cases, access to content may be restricted according to provisions established in negotiation with the copyright holder. This license shall not authorize the commercial use of the Materials by Johns Hopkins or any other person or organization, but such Materials shall be restricted to non-profit educational use. Persons may apply for commercial use by contacting the copyright holder. Copyright and any other intellectual property right in or to the Materials shall not be transferred by this agreement and shall remain with the Contributor, or the Copyright holder if different from the Contributor. Other than this limited license, the Contributor or Copyright holder retains all rights, title, copyright and other interest in the images licensed. If the submission contains material for which the Contributor does not hold copyright, the Contributor represents that s/he has obtained the permission of the Copyright owner to grant Johns Hopkins the rights required by this license, and that such third-party owned material is clearly identified and acknowledged within the text or content of the submission. If the submission is based upon work that has been sponsored or supported by an agency or organization other than Johns Hopkins, the Contributor represents that s/he has fulfilled any right of review or other obligations required by such contract or agreement. Johns Hopkins will not make any alteration, other than as allowed by this license, to your submission. This agreement embodies the entire agreement of the parties. No modification of this agreement shall be of any effect unless it is made in writing and signed by all of the parties to the agreement.\""
                          + "},"
                          + "\"agreement-to-deposit\":{"
                              + "\"type\":\"string\""
                          + "}"
                       + "}"
                     + "},"
                    + "\"options\":{"
                       + "\"fields\":{"
                          + "\"authors\":{"
                             + "\"hidden\":false"
                          + "},"
                          + "\"embargo\":{"
                             + "\"type\":\"textarea\","
                             + "\"label\":\"Deposit Agreement\","
                             + "\"disabled\":true,"
                             + "\"rows\":\"16\""
                          + "},"
                          + "\"agreement-to-deposit\":{"
                             + "\"type\":\"checkbox\","
                             + "\"rightLabel\":\"I agree to the above statement on today's date\","
                             + "\"fieldClass\":\"col-12 text-right p-0\""
                          + "}"
                       + "}"
                    + "}"
                 + "}";
        repository.setFormSchema(formSchema);
        client.updateResource(repository);
    }


    private static Consumer<URI> delete = (id) -> {
        dump(deletedDir, id);
        client.deleteResource(id);
        System.out.println("Deleted resource with URI " + id.toString());
    };
    

    // This causes us to do another fetch of the resource content, but oh well
    private static void dump(java.io.File dir, URI uri) {
        final String path = uri.getPath();

        final java.io.File dumpfile = new java.io.File(dir, path + ".nt");
        dumpfile.getParentFile().mkdirs();

        final HttpGet get = new HttpGet(uri);
        get.setHeader("Accept", "application/n-triples");

        try (FileOutputStream out = new FileOutputStream(dumpfile);
                CloseableHttpResponse response = http.execute(get)) {

            response.getEntity().writeTo(out);

        } catch (final Exception e) {
            throw new RuntimeException("Error dumping contents of " + uri, e);
        }

    }

    static CloseableHttpClient getAuthClient() {
        final CredentialsProvider provider = new BasicCredentialsProvider();
        final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(FedoraConfig.getUserName(),
                FedoraConfig.getPassword());
        provider.setCredentials(AuthScope.ANY, credentials);

        return HttpClientBuilder.create()
                .setDefaultCredentialsProvider(provider)
                .build();
    }

    
}
