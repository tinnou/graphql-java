package graphql.validation

import graphql.Scalars
import graphql.language.ObjectValue
import graphql.parser.Parser
import graphql.schema.GraphQLInputObjectField
import graphql.schema.GraphQLInputObjectType
import graphql.schema.GraphQLInputType

class SpecValidationObjectValueTest extends SpecValidationBase {

    def 'Object value is coercible into GraphQL Type'() {

        setup:
        def defaultValue = """
{
    clientMutationId: "Hello"
    video: {
      id: "ID"
    }
  }"""

        Parser parser = new Parser();
        ObjectValue objectValue = parser.parseObjectValue(defaultValue);

        GraphQLInputObjectType video = GraphQLInputObjectType.newInputObject()
                .name("Video")
                .field(GraphQLInputObjectField.newInputObjectField()
                .name("id")
                .type(Scalars.GraphQLString))
                .build();

        GraphQLInputObjectType videoType = GraphQLInputObjectType.newInputObject()
                .name("SampleVideoInput")
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name("clientMutationId")
                    .type(Scalars.GraphQLString)
                    .defaultValue(null)
                    .build())
                .field(GraphQLInputObjectField.newInputObjectField()
                    .name("video")
                    .type((GraphQLInputType) video)
                    .build())
                .build();

        expect:
        new ValidationUtil().isValidLiteralValue(objectValue, videoType)
    }
}
